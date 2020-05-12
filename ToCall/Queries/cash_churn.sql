SELECT couriers.city_code,
       geography.country_code,
       couriers.id,
       courier_wallets.cash_balance/100 as cash_balance,
       courier_names.name AS name,
       courier_phone_numbers.phone_number AS phone_number,
       --courier_emails.email AS email,
       couriers.enabled AS enabled,
       couriers.last_order_time_local
FROM couriers
LEFT JOIN courier_wallets on courier_wallets.courier_id = couriers.id
LEFT JOIN geography on geography.code = couriers.city_code
LEFT JOIN private_courier.courier_names ON courier_names.courier_id = couriers.id
LEFT JOIN private_courier.courier_phone_numbers ON courier_phone_numbers.courier_id = couriers.id
LEFT JOIN private_courier.courier_emails ON courier_emails.courier_id = couriers.id
WHERE geography.country_code IN ('MA', 'RO', 'UA', 'GE', 'KE', 'HR', 'CI', 'KZ', 'RS')
and to_char(couriers.last_order_time_local, 'YYMMDD') > to_char(current_date - INTERVAL '52 DAYS', 'YYMMDD')
and to_char(couriers.last_order_time_local, 'YYMMDD') < to_char(current_date - INTERVAL '21 DAYS', 'YYMMDD')
--and to_char(couriers.last_order_time_local, 'YYMMDD') > to_char(current_date - INTERVAL '22 DAYS', 'YYMMDD')
--and to_char(couriers.last_order_time_local, 'YYMMDD') < to_char(current_date - INTERVAL '14 DAYS', 'YYMMDD')
--and couriers.enabled=0
ORDER BY 3 DESC;
