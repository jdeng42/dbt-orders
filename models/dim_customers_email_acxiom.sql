{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('order_flash_events_location')}}
),

customer_orders as (
    select
        axs_email_hash,
        min(sale_datetime) as first_order_date,
        max(sale_datetime) as most_recent_order_date,
        COUNT(DISTINCT CASE WHEN (NOT COALESCE(pricing_mode_id = 1 , FALSE)) THEN 
        order_ticket_unique_id ELSE NULL END) AS tickets_sold_no_comps,
        COUNT(DISTINCT order_ticket_unique_id) AS number_of_tickets_sold,
        COUNT(DISTINCT order_unique_id) AS number_of_orders,
        COUNT(DISTINCT event_unique_id) AS number_of_events,

        SUM(CASE WHEN order_ticket_identifier=1 THEN amount_gross ELSE 0 END) AS total_revenue,

        SUM(FLOOR(COALESCE((CASE WHEN order_ticket_identifier=1 THEN days_sold_after_onsale ELSE 0 END), 0))) / COUNT(DISTINCT CASE WHEN days_sold_after_onsale IS NOT NULL THEN 
        order_ticket_unique_id  ELSE NULL END) AS average_days_sold_after_onsale,
        SUM(FLOOR(COALESCE((CASE WHEN order_ticket_identifier=1 THEN  days_sold_before_event ELSE 0 END), 0)))/ COUNT(DISTINCT CASE WHEN days_sold_before_event IS NOT NULL THEN 
        order_ticket_unique_id  ELSE NULL END) AS average_days_sold_before_event,

        COUNT(DISTINCT CASE WHEN (ticket_state = 'TRANSFERRED') THEN 
        ticket_id ELSE NULL END) AS count_transferred_tickets,
        COUNT(DISTINCT CASE WHEN (ticket_state = 'TRANSFERRED') THEN 
        transfer_action_id || ':' || ticket_id  ELSE NULL END) AS count_transfers,

        AVG(CASE WHEN order_ticket_identifier=1 THEN order_distance_in_km ELSE NULL END) AS average_order_distance_in_km,

        COUNT(DISTINCT venue_unique_id) AS number_of_venues,

        ROUND(COUNT(DISTINCT CASE WHEN channel='Back Office' THEN event_unique_id ELSE NULL END) *1.0 / number_of_events, 2) AS channel_back_office_percent,
        ROUND(COUNT(DISTINCT CASE WHEN (COALESCE(channel='Web', FALSE)) THEN
            event_unique_id ELSE NULL END) *1.0 / number_of_events, 2) AS channel_web_percent,

        ROUND(COUNT(DISTINCT CASE WHEN (COALESCE(major_category_name='Sports', FALSE)) THEN
            event_unique_id ELSE NULL END) *1.0 / number_of_events, 2) AS cat_sports_percent,
        ROUND(COUNT(DISTINCT CASE WHEN (COALESCE(major_category_name='Music', FALSE)) THEN
            event_unique_id ELSE NULL END) *1.0 / number_of_events, 2) AS cat_music_percent,
        ROUND(COUNT(DISTINCT CASE WHEN (COALESCE(major_category_name='Arts & Family', FALSE)) THEN
            event_unique_id ELSE NULL END) *1.0 / number_of_events, 2) AS cat_arts_family_percent,

        COUNT(DISTINCT CASE WHEN price_code_type ilike '%season%' THEN order_ticket_unique_id ELSE NULL END) AS number_of_season_tickets,

    from orders
    WHERE is_canceled is FALSE -- shall this condition live elsewhere?
    group by 1 
)
select * from customer_orders