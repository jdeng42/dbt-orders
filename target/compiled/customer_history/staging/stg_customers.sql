SELECT 
    axs_customer_id as customer_unique_id,
    axs_email_hash,
    -- left(zip, 5) as zip -- eleminate situation as 01234-1234
    zip_code as zip
FROM analytics.demographics_all -- instead of ticketing.customers

--  no need to join SQL at this moment
--     CASE WHEN b.email is not null THEN 1 ELSE 0 END AS is_broker
-- FROM ticketing.customers c LEFT JOIN analytics.yield_manager_partners b 
-- on lower(c.email)=lower(b.email)