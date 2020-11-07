-- create temporary staging table
CREATE OR REPLACE TEMP TABLE playground.jaffle_shop.fct_orders__temp AS
SELECT  src.order_id,
        src.load_id,
        MD5_HEX(CONCAT(NVL(TO_CHAR(src.status), 'N/A'))) AS md5_hash,
        src.status,
        src.updated_at::TIMESTAMP_NTZ AS valid_from,
        NULL::TIMESTAMP_NTZ AS valid_to
 FROM (
    select
            load_id,
            order_id,
            updated_at,
            status
    from playground.jaffle_shop.stg_orders
) AS src;


-- start the merge of the staging entries to the existing order status change log
START TRANSACTION name merge_history;

UPDATE playground.jaffle_shop.fct_orders dst
SET valid_to = src.valid_from
FROM (
        SELECT dst.id, MIN(src.valid_from) AS valid_from
        FROM playground.jaffle_shop.fct_orders dst
        JOIN playground.jaffle_shop.fct_orders__temp src ON dst.order_id = src.order_id
        WHERE dst.md5_hash != src.md5_hash AND dst.load_id < src.load_id AND dst.valid_to IS NULL
        GROUP BY dst.id
) src
WHERE src.id=dst.id;



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