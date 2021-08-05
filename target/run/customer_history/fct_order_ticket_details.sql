

  create  table "data_platform_prod"."data_science"."fct_order_ticket_details__dbt_tmp"
  as (
    

with cst as (
    select * from "data_platform_prod"."data_science"."order_ticket_details_cst"
),
est as (
    select * from "data_platform_prod"."data_science"."order_ticket_details_est"
),
mst as (
    select * from "data_platform_prod"."data_science"."order_ticket_details_mst"
),
pst as (
    select * from "data_platform_prod"."data_science"."order_ticket_details_pst"
),
final as (
    SELECT * from cst
    UNION
    SELECT * from est
    UNION
    SELECT * from mst
    UNION
    SELECT * from pst
)

SELECT * FROM final
  );