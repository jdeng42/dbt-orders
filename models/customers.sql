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
        
        -- COUNT(DISTINCT CASE WHEN (NOT COALESCE(is_canceled , FALSE)) AND 
        -- (NOT COALESCE(pricing_mode_id = 1 , FALSE)) 
        -- THEN order_ticket_unique_id ELSE NULL END) AS tickets_sold_no_comps,
        -- COUNT(DISTINCT CASE WHEN NOT COALESCE(is_canceled , FALSE) 
        -- THEN order_ticket_unique_id ELSE NULL END) AS number_of_tickets_sold,
        -- COUNT(DISTINCT CASE WHEN NOT COALESCE(is_canceled , FALSE) 
        -- THEN order_unique_id ELSE NULL END) AS number_of_orders,
        -- SUM(DISTINCT CASE WHEN NOT COALESCE(is_canceled , FALSE) 
        -- THEN amount_gross ELSE NULL END) AS total_revenue
        COUNT(DISTINCT CASE WHEN (NOT COALESCE(pricing_mode_id = 1 , FALSE)) 
        THEN order_ticket_unique_id ELSE NULL END) AS tickets_sold_no_comps,
        COUNT(DISTINCT order_ticket_unique_id) AS number_of_tickets_sold,
        COUNT(DISTINCT order_unique_id) AS number_of_orders,
        SUM(DISTINCT amount_gross) AS total_revenue

    from order_tickets
    group by 1
),
final as (
    select
        customers.customer_unique_id,
        customers.email,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.tickets_sold_no_comps, 0) as tickets_sold_no_comps,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        coalesce(customer_orders.number_of_tickets_sold, 0) as number_of_tickets_sold,
        coalesce(customer_orders.total_revenue, 0) as total_revenue
    from customers
    left join customer_orders using (customer_unique_id)
)
select * from final