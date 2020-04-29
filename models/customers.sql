with customers as (
    select * from {{ ref('customer_broker')}}
),
order_flash as (
    select * from {{ ref('order_flash_event')}}
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
        SUM(amount_gross) AS total_revenue,
        COUNT(DISTINCT CASE WHEN (ticket_state = 'TRANSFERRED') THEN 
        ticket_id ELSE NULL END) AS count_transferred_tickets,
        COUNT(DISTINCT CASE WHEN (ticket_state = 'TRANSFERRED') THEN 
        transfer_action_id || ':' || ticket_id  ELSE NULL END) AS count_transfers,

        SUM(FLOOR(COALESCE(datediff(days, onsale_date, sale_datetime), 0))) / COUNT(DISTINCT CASE WHEN (datediff(days, onsale_date, sale_datetime))IS NOT NULL THEN 
        order_ticket_unique_id  ELSE NULL END) AS average_days_sold_after_onsale,
        SUM(FLOOR(COALESCE(datediff(days, sale_datetime, event_datetime), 0)))/ COUNT(DISTINCT CASE WHEN (datediff(days, sale_datetime, event_datetime))IS NOT NULL THEN 
        order_ticket_unique_id  ELSE NULL END) AS average_days_sold_before_event

    from order_flash
    group by 1
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
        customer_orders.total_revenue,
        average_days_sold_after_onsale,
        average_days_sold_before_event,
        customer_orders.count_transferred_tickets,
        customer_orders.count_transfers
    from customers
    left join customer_orders using (customer_unique_id)
)
select * from final