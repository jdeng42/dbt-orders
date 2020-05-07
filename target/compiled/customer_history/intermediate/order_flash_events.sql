with orders as (
    select * from "data_platform_prod"."data_science"."stg_order"
),
flash as (
    select * from "data_platform_prod"."data_science"."stg_flash"
),
events as (
    select * from "data_platform_prod"."data_science"."stg_events"
),
order_flash as (
    SELECT *
    from orders LEFT JOIN flash ON flash.fk_order_unique_id=orders.order_unique_id
        and flash.fk_seat_unique_id=orders.seat_unique_id
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
    events.event_unique_id,
    ticket_id,
    ticket_state,
    datediff(days, onsale_date, sale_datetime) AS days_sold_after_onsale,
    datediff(days, sale_datetime, event_datetime) AS days_sold_before_event,
    is_canceled
    FROM order_flash INNER JOIN events USING (event_unique_id)
)

SELECT * FROM final
WHERE is_canceled is FALSE -- shall this condition live else?