with order_flash as (
    select * from {{ref('order_flash')}}
),
events as (
    select * from {{ref('stg_events')}}
)

select * FROM order_flash INNER JOIN events USING (event_unique_id)