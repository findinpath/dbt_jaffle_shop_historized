{#
    Creates a MD5 function call with all filed_names as parameter including null being converted to the string "N/A"
#}
{% macro create_md5_expression(field_names, prefix=None) %}
    {%- set prefixed_field_names = [] -%}
    {%- if prefix is none -%}
        {% set prefix_with_default = '' -%}
    {%- else -%}
        {% set prefix_with_default = prefix -%}
    {%- endif -%}

    {%- for field_name in field_names -%}
        {%- do prefixed_field_names.append("NVL(TO_CHAR(" ~ prefix_with_default ~ field_name ~ "), 'N/A')") -%}
    {%- endfor -%}

    MD5_HEX(CONCAT({{prefixed_field_names|join(",'|', ")}}))

{% endmacro %}

{% macro build_historized_temp_table(tmp_relation,
                                    primary_key_field_name,
                                    load_id_column_name,
                                    valid_from_column_name,
                                    sql) %}
    {% do log("Build temporary table '" ~ tmp_relation ~ "' to be used in the historization process.") %}

    {% set (stage_exists, stage_relation) = get_or_create_relation(tmp_relation.database, tmp_relation.schema, tmp_relation.identifier ~ '_v', 'view') %}
    {% call statement('create_stage_view') %}
        CREATE OR REPLACE VIEW {{ stage_relation }} AS
        {{ sql }}
    {% endcall %}

    {% set columns = adapter.get_columns_in_relation(stage_relation) %}

    {% call statement('drop_stage_view') %}
        DROP VIEW {{ stage_relation }};
    {% endcall %}

    {%- set history_coordinates_col_names = [] -%}
    {%- do history_coordinates_col_names.append(primary_key_field_name.lower()) -%}
    {%- do history_coordinates_col_names.append(valid_from_column_name.lower()) -%}
    {%- do history_coordinates_col_names.append(load_id_column_name.lower()) -%}

    {%- set md5_hash_col_names = [] -%}
    {%- for column in columns -%}
        {%- if not column.name.lower() in history_coordinates_col_names -%}
            {%- do md5_hash_col_names.append(column.name.lower()) -%}
        {%- endif -%}
    {%- endfor -%}

    CREATE OR REPLACE TEMP TABLE {{ tmp_relation }} AS
    SELECT  src.{{primary_key_field_name}},
            src.{{load_id_column_name}},
            {{create_md5_expression(md5_hash_col_names, 'src.')}} AS md5_hash,
            {% for column_name in md5_hash_col_names %}src.{{column_name}},{% endfor %}
            src.{{valid_from_column_name}}::TIMESTAMP_NTZ AS valid_from,
            NULL::TIMESTAMP_NTZ AS valid_to
     FROM (
            {{sql}}
    ) AS src;
{% endmacro %}

{#
   Creates (or replaces it if exists) the target relation
#}
{% macro create_historized_table(relation,
                             tmp_relation,
                             valid_from_column_name) %}
    {%- do log("Build historized table '" ~ relation ~ "'") -%}
    {%- set (stage_exists, stage_relation) = get_or_create_relation(relation.database, relation.schema, relation.identifier ~ '_tmp_v', 'view') -%}
    {%- set tmp_rel_columns = adapter.get_columns_in_relation(tmp_relation) -%}

    CREATE SEQUENCE IF NOT EXISTS {{ relation.schema }}.{{relation.identifier}}_seq;
    CREATE TABLE {{relation}} (
        id NUMBER DEFAULT {{ relation.schema }}.{{relation.identifier}}_seq.nextval,
        {%- for column in tmp_rel_columns|list -%}
            {{column.name}} {{column.data_type}},
        {%- endfor %}
        CONSTRAINT {{relation.identifier}}_pk PRIMARY KEY (id)
    );
{% endmacro %}

{#
   Merge the temporary data into the target historized target table
#}
{% macro merge_history(source_relation,
                       target_relation,
                       primary_key_column_name,
                       load_id_column_name,
                       valid_from_column_name) %}
    {%- do log("Merge source table '" ~ source_relation ~ "' into '" ~ target_relation ~ "' historized table.") -%}
    {%- set source_relation_columns = adapter.get_columns_in_relation(source_relation) -%}

    START TRANSACTION name merge_history;


    {# update valid_to for the unbounded records when a newer corresponding record with changed attributes arrived #}
    UPDATE {{target_relation}} dst
    SET valid_to = src.valid_from
    FROM (
            SELECT dst.id, MIN(src.valid_from) AS valid_from
            FROM {{target_relation}} dst
            JOIN {{source_relation}} src ON dst.{{primary_key_column_name}} = src.{{primary_key_column_name}}
            WHERE dst.md5_hash != src.md5_hash AND dst.load_id < src.load_id AND dst.valid_to IS NULL
            GROUP BY dst.id
    ) src
    WHERE src.id=dst.id;

    {# generate list of columns to be included in the md5 hash calculation #}
    {% set src_column_list_without_validity_range_columns = [] %} {# without valid_from & valid_to fields #}
    {%- for column in source_relation_columns -%}
        {%- if column.name.lower() != "valid_from"
                and column.name.lower() != "valid_to"  -%}
            {%- do src_column_list_without_validity_range_columns.append(column.name) -%}
        {%- endif -%}
    {%- endfor %}

    {# insert new records into the target relation #}
    INSERT INTO {{target_relation}} ({{ src_column_list_without_validity_range_columns|join(", ") }}, valid_from, valid_to)
    SELECT {{ src_column_list_without_validity_range_columns|join(", ") }}, valid_from, valid_to
    FROM (
        SELECT DISTINCT src.{{ src_column_list_without_validity_range_columns|join(", src.") }}, src.valid_from, src.valid_to,
                LAST_VALUE(dst.valid_to) OVER (
                    PARTITION BY dst.{{primary_key_column_name}}
                    ORDER BY dst.valid_from) AS max_dst_valid_to,
                LAST_VALUE(dst.valid_from) OVER (
                    PARTITION BY dst.{{primary_key_column_name}}
                    ORDER BY dst.valid_from) AS max_dst_valid_from,
                LAST_VALUE(dst.md5_hash) OVER (
                    PARTITION BY dst.{{primary_key_column_name}}
                    ORDER BY dst.valid_from) AS last_dst_md5_hash,
                DENSE_RANK() OVER (
                    PARTITION BY dst.{{primary_key_column_name}}
                    ORDER BY src.valid_from) AS src_row_number
        FROM (
            SELECT {{ src_column_list_without_validity_range_columns|join(", ") }}, valid_from, next_valid_from AS valid_to
            FROM (
                {#  obtain the validity range for each of the new records #}
                SELECT {{ src_column_list_without_validity_range_columns|join(", ") }}, valid_from,
                        LEAD(valid_from) OVER (PARTITION BY {{primary_key_column_name}}
                                            ORDER BY valid_from, {{load_id_column_name}}) next_valid_from
                FROM (
                    {# prepare input to filter out eventual subsequent duplicated entries
                    (based on the md5_hash of the columns containing the historized attributes) #}
                    SELECT {{ src_column_list_without_validity_range_columns|join(", ") }}, valid_from,
                            LAG(md5_hash) OVER (PARTITION BY {{primary_key_column_name}}
                                                ORDER BY valid_from, {{load_id_column_name}}) prev_md5_hash,
                            LEAD(md5_hash) OVER (PARTITION BY {{primary_key_column_name}}
                                                 ORDER BY valid_from, {{load_id_column_name}}) next_md5_hash
                    FROM {{source_relation}}
                )
                WHERE (md5_hash != NVL(prev_md5_hash, 'N/A'))
                  AND (load_id  > (SELECT NVL(MAX(load_id),0) FROM  {{target_relation}}))
            )
        ) AS src
        LEFT JOIN {{target_relation}} dst ON dst.{{primary_key_column_name}}=src.{{primary_key_column_name}}
    )
    WHERE (valid_from>=max_dst_valid_to OR max_dst_valid_to IS NULL)
      AND (valid_from>=max_dst_valid_from OR max_dst_valid_to IS NULL)
      AND ((src_row_number=1 AND last_dst_md5_hash != md5_hash) OR src_row_number>1 OR last_dst_md5_hash IS NULL);

    COMMIT;
{% endmacro %}


{% materialization historized, adapter='snowflake' %}
    {% set config = model.get('config') %}
    {% set sql = model.get('injected_sql') %}

    {% set target_database = model.get('database') %}
    {% set target_schema = model.get('schema') %}
    {% set target_table = model.get('alias', model.get('name')) %}
    {% set temp_table = target_table ~ "__dbt_tmp" %}

    {% set full_refresh_mode = (flags.FULL_REFRESH == True) %}


    {% set old_relation = adapter.get_relation(database=target_database, schema=target_schema, identifier=target_table) %}

    {#  the name for the column used to identify the staged entity. NOTE that there is no support in this macro for a primary key with multiple columns. #}
    {% set primary_key_column_name = config.get('primary_key_column_name') %}

    {#
       Name of the column containing monotonically increasing sequence values.
       This column is important to use in order to be able to distinguish between order
       changes that happen within the same timestamp. Even though it is unlikely, there
       can happen that two entity changes happen in the same timestamp.
       In order to distinguish which is the latest entity change, the load id sequence identifier
       is used to distinguish between the changes. A sequence identifier
       ensures that there is a precedence for the staged entries independent of the timestamp
       when they happened.
    #}
    {% set load_id_column_name = config.get('load_id_column_name', 'load_id') %}

    {# the column containing the timestamp when the staged entity has been last updated. #}
    {% set valid_from_column_name = config.get('valid_from_column_name', 'valid_from') %}

    {% if old_relation is not none and full_refresh_mode -%}
        {{ adapter.drop_relation(old_relation) }}
        {% set old_relation = none %}
    {%- endif %}

    {% set original_query_tag = set_query_tag() %}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% if not adapter.check_schema_exists(target_database, target_schema) %}
        {% do create_schema(target_database, target_schema) %}
    {% endif %}

    {% set target_relation_exists, target_relation = get_or_create_relation(database=target_database,
                                                                            schema=target_schema,
                                                                            identifier=target_table,
                                                                            type='table') %}

    {% set source_relation = api.Relation.create(database=target_relation.database,
                                               schema=target_relation.schema,
                                               identifier=target_relation.identifier ~ "__temp",
                                               type=target_relation.type
                                              ) %}
    {% call statement() %}
        {{ build_historized_temp_table(source_relation,
                                      primary_key_column_name,
                                      load_id_column_name,
                                      valid_from_column_name,
                                      sql) }}
    {% endcall %}

    {% if full_refresh_mode or old_relation is none -%}
        {% call statement() %}
            {{ create_historized_table(target_relation,
                                   source_relation,
                                   valid_from_column_name) }}
        {% endcall %}
    {%- endif %}

    {% call statement('main') %}
        {{ merge_history(source_relation,
                         target_relation,
                         primary_key_column_name,
                         load_id_column_name,
                         valid_from_column_name) }}
    {% endcall %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {{ adapter.commit() }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {% do unset_query_tag(original_query_tag) %}

    {% do return({'relations': [target_relation]}) %}
{% endmaterialization %}
