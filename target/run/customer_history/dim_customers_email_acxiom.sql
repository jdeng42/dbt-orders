

  create  table "data_platform_prod"."data_science"."dim_customers_email_acxiom__dbt_tmp"
  as (
    

with orders as (
    select * from "data_platform_prod"."data_science"."fct_order_ticket_details"
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

        ROUND(COUNT(DISTINCT CASE WHEN channel='Back Office' THEN order_ticket_unique_id ELSE NULL END) *1.0 / number_of_tickets_sold, 2) AS channel_back_office_percent,
        ROUND(COUNT(DISTINCT CASE WHEN channel='Web' THEN
            order_ticket_unique_id ELSE NULL END) *1.0 / number_of_tickets_sold, 2) AS channel_web_percent,

        ROUND(COUNT(DISTINCT CASE WHEN major_category_name='Sports' THEN
            event_unique_id ELSE NULL END) *1.0 / number_of_events, 2) AS cat_sports_percent,
        ROUND(COUNT(DISTINCT CASE WHEN major_category_name='Music' THEN
            event_unique_id ELSE NULL END) *1.0 / number_of_events, 2) AS cat_music_percent,
        ROUND(COUNT(DISTINCT CASE WHEN major_category_name='Arts & Family' THEN
            event_unique_id ELSE NULL END) *1.0 / number_of_events, 2) AS cat_arts_family_percent,

        ROUND(COUNT(DISTINCT CASE WHEN venue_type='Arena' THEN
            event_unique_id ELSE NULL END) *1.0 / number_of_events, 2) AS venue_arena_percent,
        ROUND(COUNT(DISTINCT CASE WHEN venue_type='Large Music Venue' THEN
            event_unique_id ELSE NULL END) *1.0 / number_of_events, 2) AS venue_large_music_percent,
        ROUND(COUNT(DISTINCT CASE WHEN venue_type='Club and Theater' THEN
            event_unique_id ELSE NULL END) *1.0 / number_of_events, 2) AS venue_club_theatre_percent,

        COUNT(DISTINCT CASE WHEN is_season_ticket = 1 THEN order_ticket_unique_id ELSE NULL END) AS number_of_season_tickets
        -- COUNT(DISTINCT CASE WHEN price_code_type ilike '%season%' THEN order_ticket_unique_id ELSE NULL END) AS number_of_season_tickets

    from orders
    WHERE is_canceled is FALSE -- shall this condition live elsewhere?
    group by 1 
)
select * from customer_orders
  );