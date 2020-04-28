SELECT
    ticket_state,
    ticket_id,
    transfer_action_id,
    fk_order_unique_id,
    fk_seat_unique_id
FROM
    flash.tickets LEFT JOIN flash.forwards USING (ticket_id)