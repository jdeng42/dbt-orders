-- calculate distance between customer location vs event location -- 

with orders as (
    SELECT * FROM "data_platform_prod"."data_science"."order_flash_events_cst"       
),
customers as (
    SELECT * FROM "data_platform_prod"."data_science"."stg_customers"
),
zipcodes as (
    select zipcode, longitude, latitude from public.us_zipcodes
    where primary_record='P'
),
-- customers_zip as (
--     Select
--         customers.*,
--         CAST(longitude AS DOUBLE PRECISION) as customer_long,
--         CAST(latitude AS DOUBLE PRECISION) as customer_lat
--     FROM customers LEFT JOIN zipcodes ON customers.zip=zipcodes.zipcode
-- ),
-- orders_zip as (
--     Select
--         orders.*,
--         CAST(longitude AS DOUBLE PRECISION) as venue_long,
--         CAST(latitude AS DOUBLE PRECISION) as venue_lat
--     FROM orders LEFT JOIN zipcodes ON orders.venue_zip=zipcodes.zipcode
-- ),
final as (
    SELECT
        customer_unique_id,
        axs_email_hash,
        order_ticket_unique_id,
        ROW_NUMBER() over (PARTITION BY order_ticket_unique_id ORDER BY 
        order_ticket_unique_id) AS order_ticket_identifier, -- to remove duplicate order_ticket_unique_id
        order_unique_id,
        amount_gross,
        channel,
        sale_datetime,
        pricing_mode_id,
        -- price_code_type,
        is_season_ticket,
        transfer_action_id,
        event_unique_id,
        ticket_id,
        ticket_state,
        days_sold_after_onsale,
        days_sold_before_event,
        venue_unique_id,
        venue_type,
        major_category_name,
        is_canceled,
        round(ST_DistanceSphere(ST_Point(CAST(c_zip.longitude AS DOUBLE PRECISION), CAST(c_zip.latitude AS DOUBLE PRECISION)), 
        ST_Point(CAST(v_zip.longitude AS DOUBLE PRECISION), CAST(v_zip.latitude AS DOUBLE PRECISION))) / 1000, 0) AS order_distance_in_km,
        order_distance_in_km / 1.6 AS order_distance_in_miles
    from customers
    INNER join orders using (customer_unique_id)
    LEFT JOIN zipcodes c_zip ON customers.zip=c_zip.zipcode
    LEFT JOIN zipcodes v_zip ON orders.venue_zip=v_zip.zipcode
)

SELECT * FROM final