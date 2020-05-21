-- calculate distance between customer location vs event location
{{ config(materialized='table') }}

with orders as (
    SELECT * FROM {{ ref('order_flash_events')}}       
),
customers as (
    SELECT
        customer_unique_id,
        zip
    FROM {{ref('stg_customers')}}
),
zipcodes as (
    select zipcode, longitude, latitude from public.us_zipcodes
    where primary_record='P'
),
customers_zip as (
    Select
        customers.*,
        CAST(longitude AS DOUBLE PRECISION) as customer_long,
        CAST(latitude AS DOUBLE PRECISION) as customer_lat
    FROM customers LEFT JOIN zipcodes ON customers.zip=zipcodes.zipcode
),
orders_zip as (
    Select
        orders.*,
        CAST(longitude AS DOUBLE PRECISION) as venue_long,
        CAST(latitude AS DOUBLE PRECISION) as venue_lat
    FROM orders LEFT JOIN zipcodes ON orders.venue_zip=zipcodes.zipcode
),
final as (
    SELECT
        customer_unique_id,
        order_ticket_unique_id,
        order_unique_id,
        amount_gross,
        sale_datetime,
        pricing_mode_id,
        transfer_action_id,
        event_unique_id,
        ticket_id,
        ticket_state,
        days_sold_after_onsale,
        days_sold_before_event,
        is_canceled,
        round(ST_DistanceSphere(ST_Point(customer_long, customer_lat), 
        ST_Point(venue_long, venue_lat)) / 1000, 0) AS order_distance_in_km
    from customers_zip
    INNER join orders_zip using (customer_unique_id)
)
select * FROM final
WHERE is_canceled is FALSE -- shall this condition live elsewhere?