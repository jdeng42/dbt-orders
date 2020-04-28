
  create view "data_platform_prod"."data_science"."stg_customers__dbt_tmp" as (
    select
    customer_unique_id,
    email,
    first_name,
    last_name
from ticketing.customers
  );
