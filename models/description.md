{% docs table_customers %}

This table calculates aggregated order information for customers. Notice it is based on condition specified on previous stages, e.g.
* it doesn't include canceled events (stg_orders)
* it only includes ticket purchase (stg_orders)
* it excludes test events (stg_events)

It group by email address, noticing one email could associate with multiple customer_id