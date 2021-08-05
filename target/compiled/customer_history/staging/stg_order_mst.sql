select
    order_ticket_unique_id,
    order_unique_id,
    customer_unique_id,
    amount_gross,
    channel,
    sale_datetime,
    -- zone_unique_id,
    pricing_mode_id,
    -- price_code_type,
    CASE WHEN price_code_type ilike '%season%' THEN 1 ELSE 0 END AS is_season_ticket,
    seat_unique_id,
    ticketing.order_tickets.event_unique_id,
    is_canceled
from ticketing.order_tickets
INNER JOIN config.context_configuration config USING(context_id)
INNER JOIN ticketing.price_codes USING(price_code_unique_id)
INNER JOIN ticketing.zones USING (zone_unique_id)
WHERE 
config.timezone = 'MST' and lower(zone_type_description) in ('admissions', 'premium seating')