{{ config(materialized='table') }}

with customers as (
    select * from {{ ref('stg_customers')}}
),
orders as (
    select * from {{ ref('order_flash_events_location')}}
),

customer_orders as (
    select
        customer_unique_id,
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
    group by 1 -- Not group by email address as some accounts don't have associated email
),
final as (
    select
        customers.customer_unique_id,
        customers.email,
        customers.is_broker,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        customer_orders.tickets_sold_no_comps,
        customer_orders.number_of_orders,
        customer_orders.number_of_tickets_sold,
        customer_orders.number_of_events,
        customer_orders.total_revenue,
        average_days_sold_after_onsale,
        average_days_sold_before_event,
        customer_orders.count_transferred_tickets,
        customer_orders.count_transfers,
        average_order_distance
    from customers
    left join customer_orders using (customer_unique_id)
)
select * from final