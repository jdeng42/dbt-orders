with orders as (
    select * from {{ ref('stg_order_pst')}}
),
events as (
    select * from {{ ref('stg_events')}}
),
flash as (
    select * from {{ ref('stg_flash')}}
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
    channel,
    sale_datetime,
    pricing_mode_id,
    is_season_ticket,
    transfer_action_id,
    events.event_unique_id,
    ticket_id,
    ticket_state,
    venue_unique_id,
    venue_zip,
    venue_type,
    datediff(days, onsale_date, sale_datetime) AS days_sold_after_onsale,
    datediff(days, sale_datetime, event_datetime) AS days_sold_before_event,
    major_category_name,
    is_canceled
    FROM order_flash INNER JOIN events USING (event_unique_id)
)

SELECT * FROM final