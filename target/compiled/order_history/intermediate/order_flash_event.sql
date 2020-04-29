with order_flash as (
    select * from "data_platform_prod"."data_science"."order_flash"
),
events as (
    select * from "data_platform_prod"."data_science"."stg_events"
)

select * FROM order_flash INNER JOIN events USING (event_unique_id)