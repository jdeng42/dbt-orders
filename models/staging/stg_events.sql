SELECT
    event_unique_id,
    onsale_date,
    event_datetime
FROM
    ticketing.events
    INNER JOIN analytics.event_onsale USING (event_unique_id)
WHERE event_name NOT ilike 'test event%'
      AND event_name NOT ilike '%base event%'
      AND event_name NOT ilike '% test event%'
      AND event_name NOT ilike '%- RR Base%'
      AND (nvl(ticketing.events.is_exclude,false)) is false