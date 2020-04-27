with customers as (
    select * from {{ ref('stg_customers')}}
),
order_tickets as (
    select * from {{ ref('stg_orders_aggregate')}}
),
customer_orders as (
    select
        customer_unique_id,
        min(sale_datetime) as first_order_date,
        max(sale_datetime) as most_recent_order_date,
        COUNT(order_unique_id) as number_of_orders,
        COUNT(order_ticket_unique_id) AS tickets_purchased,
        SUM(amount_gross) AS total_revenue
    from order_tickets
    group by 1
),
final as (
    select
        customers.customer_unique_id,
        customers.email,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        coalesce(customer_orders.tickets_purchased, 0) as ticket_purchased,
        customer_orders.total_revenue
    from customers
    left join customer_orders using (customer_unique_id)
)
select * from final