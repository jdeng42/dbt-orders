{{ config(materialized='table') }}

with cst as (
    select * from {{ ref('order_flash_events_cst')}}
),
est as (
    select * from {{ ref('order_flash_events_est')}}
),
mst as (
    select * from {{ ref('order_flash_events_mst')}}
),
pst as (
    select * from {{ ref('order_flash_events_pst')}}
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


