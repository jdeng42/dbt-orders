
  create view "data_platform_prod"."data_science"."stg_order_flash__dbt_tmp" as (
    with orders as (
    select * from "data_platform_prod"."data_science"."stg_order"
),
flash as (
    select * from "data_platform_prod"."data_science"."stg_flash"
),
final as (
    SELECT
    order_ticket_unique_id,
    order_unique_id,
    customer_unique_id,
    amount_gross,
    sale_datetime,
    pricing_mode_id,
    transfer_action_id,
    ticket_id,
    ticket_state
    from orders LEFT JOIN flash ON flash.fk_order_unique_id=orders.order_unique_id
        and flash.fk_seat_unique_id=orders.seat_unique_id
)
select * from final
  );
