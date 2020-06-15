{{ config(materialized='table') }}

with customers as (
    select * from {{ ref('stg_customers')}}
),
orders as (
    select * from {{ ref('order_flash_events_location')}}
),

customer_orders as (
    select
        email,
        min(sale_datetime) as first_order_date,
        max(sale_datetime) as most_recent_order_date,
        COUNT(DISTINCT CASE WHEN (NOT COALESCE(pricing_mode_id = 1 , FALSE)) THEN 
        order_ticket_unique_id ELSE NULL END) AS tickets_sold_no_comps,
        COUNT(DISTINCT order_ticket_unique_id) AS number_of_tickets_sold,
        COUNT(DISTINCT order_unique_id) AS number_of_orders,
        COUNT(DISTINCT event_unique_id) AS number_of_events,
        SUM(amount_gross) AS total_revenue,

        SUM(FLOOR(COALESCE(days_sold_after_onsale, 0))) / COUNT(DISTINCT CASE WHEN days_sold_after_onsale IS NOT NULL THEN 
        order_ticket_unique_id  ELSE NULL END) AS average_days_sold_after_onsale,
        SUM(FLOOR(COALESCE(days_sold_before_event, 0)))/ COUNT(DISTINCT CASE WHEN days_sold_before_event IS NOT NULL THEN 
        order_ticket_unique_id  ELSE NULL END) AS average_days_sold_before_event,

        COUNT(DISTINCT CASE WHEN (ticket_state = 'TRANSFERRED') THEN 
        ticket_id ELSE NULL END) AS count_transferred_tickets,
        COUNT(DISTINCT CASE WHEN (ticket_state = 'TRANSFERRED') THEN 
        transfer_action_id || ':' || ticket_id  ELSE NULL END) AS count_transfers,

        AVG(order_distance_in_km) AS average_order_distance

    from orders
    group by 1 
)
select * from customer_orders