
  create view "data_platform_prod"."data_science"."order_flash_event__dbt_tmp" as (
    with order_flash as (
    select * from "data_platform_prod"."data_science"."order_flash"
),
events as (
    select * from "data_platform_prod"."data_science"."stg_events"
)

select * FROM order_flash INNER JOIN events USING (event_unique_id)
  );
