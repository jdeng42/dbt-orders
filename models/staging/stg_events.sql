with events as (
    SELECT
        event_unique_id,
        onsale_date,
        event_datetime,
        venue_unique_id
    FROM
        ticketing.events
        INNER JOIN analytics.event_onsale USING (event_unique_id)
    WHERE event_name NOT ilike 'test event%'
        AND event_name NOT ilike '%base event%'
        AND event_name NOT ilike '% test event%'
        AND event_name NOT ilike '%- RR Base%'
        AND (nvl(ticketing.events.is_exclude,false)) is false
),
venues as (
    SELECT
        venue_unique_id,
        venue_zip
        from ticketing.venues 
),
final as (
    SELECT
        event_unique_id,
        onsale_date,
        event_datetime,
        venue_zip
    FROM events INNER JOIN venues USING (venue_unique_id)
)
SELECT * FROM final


