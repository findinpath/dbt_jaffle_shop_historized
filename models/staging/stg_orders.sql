with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ ref('raw_orders') }}

),

renamed as (

    select
        load_id,
        order_id,
        updated_at,
        status

    from source

)

select * from renamed
