{{
    config(
        materialized = 'historized',
        primary_key_column_name = 'order_id',
        valid_from_column_name = 'updated_at',
        load_id_column_name = 'load_id'
    )
}}

select
        load_id,
        order_id,
        updated_at,
        status
from {{ ref('stg_orders') }}
