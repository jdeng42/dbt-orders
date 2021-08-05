{{ config(materialized='table') }}

with cst as (
    select * from {{ ref('order_ticket_details_cst')}}
),
est as (
    select * from {{ ref('order_ticket_details_est')}}
),
mst as (
    select * from {{ ref('order_ticket_details_mst')}}
),
pst as (
    select * from {{ ref('order_ticket_details_pst')}}
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


