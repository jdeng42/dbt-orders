with customers as (
    select * from {{ ref('customer_broker')}}
),
order_flash as (
    select * from {{ ref('order_flash')}}
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
        SUM(DISTINCT amount_gross) AS total_revenue,
        COUNT(DISTINCT CASE WHEN (ticket_state = 'TRANSFERRED') THEN 
        ticket_id ELSE NULL END) AS count_transferred_tickets,
        COUNT(DISTINCT CASE WHEN (ticket_state = 'TRANSFERRED') THEN 
        transfer_action_id || ':' || ticket_id  ELSE NULL END) AS count_transfers

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
        coalesce(customer_orders.tickets_sold_no_comps, 0) as tickets_sold_no_comps,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        coalesce(customer_orders.number_of_tickets_sold, 0) as number_of_tickets_sold,
        coalesce(customer_orders.total_revenue, 0) as total_revenue,
        coalesce(customer_orders.count_transferred_tickets, 0) as count_transferred_tickets,
        coalesce(customer_orders.count_transfers, 0) as count_transfers
    from customers
    left join customer_orders using (customer_unique_id)
)
select * from final