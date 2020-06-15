SELECT 
    customer_unique_id,
    email,
    left(zip, 5) as zip -- eleminate situation as 01234-1234
FROM ticketing.customers

--  no need to join SQL at this moment
--     CASE WHEN b.email is not null THEN 1 ELSE 0 END AS is_broker
-- FROM ticketing.customers c LEFT JOIN analytics.yield_manager_partners b 
-- on lower(c.email)=lower(b.email)