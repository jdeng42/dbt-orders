
  create view "data_platform_prod"."data_science"."stg_order__dbt_tmp" as (
    select
    order_ticket_unique_id,
    order_unique_id,
    customer_unique_id,
    amount_gross,
    sale_datetime,
    zone_unique_id,
    pricing_mode_id,
    seat_unique_id,
    ticketing.order_tickets.event_unique_id,
    is_canceled
from ticketing.order_tickets
INNER JOIN ticketing.price_codes USING(price_code_unique_id)
INNER JOIN ticketing.zones USING (zone_unique_id)
WHERE 
lower(zone_type_description)  in ('admissions', 'premium seating') AND 
is_canceled is FALSE -- where shall this condition lives?
  );
