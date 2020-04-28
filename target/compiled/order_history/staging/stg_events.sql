SELECT
    event_unique_id
FROM
    ticketing.events
WHERE event_name NOT ilike 'test event%'
      AND event_name NOT ilike '%base event%'
      AND event_name NOT ilike '% test event%'
      AND event_name NOT ilike '%- RR Base%'