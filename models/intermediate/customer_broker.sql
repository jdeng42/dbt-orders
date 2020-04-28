with customers as (
    select * from {{ ref('stg_customers')}}
),

brokers as (
    SELECT email as broker_email
    FROM analytics.yield_manager_partners
),

final as (
    SELECT 
    customer_unique_id,
    email,
    broker_email,
    first_name,
    last_name
    FROM customers LEFT JOIN brokers on lower(customers.email)=brokers.broker_email
)
select * from final