
  create view "data_platform_prod"."data_science"."stg_events__dbt_tmp" as (
    SELECT
    event_unique_id
FROM
    ticketing.events
WHERE event_name NOT ilike 'test event%'
      AND event_name NOT ilike '%base event%'
      AND event_name NOT ilike '% test event%'
      AND event_name NOT ilike '%- RR Base%'
  );
