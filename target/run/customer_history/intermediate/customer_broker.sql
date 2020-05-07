
  create view "data_platform_prod"."data_science"."customer_broker__dbt_tmp" as (
    with customers as (
    select * from "data_platform_prod"."data_science"."stg_customers"
),

brokers as (
    SELECT email as broker_email
    FROM analytics.yield_manager_partners
),

final as (
    SELECT 
    customer_unique_id,
    email,
    CASE WHEN broker_email is not null THEN 1 ELSE 0 END AS is_broker,
    first_name,
    last_name
    FROM customers LEFT JOIN brokers on lower(customers.email)=brokers.broker_email
)
select * from final
  );
