
    WITH courier_data AS (
        SELECT couriers.id AS courier_id,
               to_char(couriers.first_order_time_local, 'YYYY-MM-DD') as first_order_date,
               couriers.city_code AS city_code,
               COUNT(DISTINCT CASE WHEN (orders.final_status = 'DeliveredStatus') AND (orders.deleted  = 0) THEN orders.id  ELSE NULL END) AS number_of_delivered_orders,
               CASE WHEN (number_of_delivered_orders <= 25) THEN TRUE ELSE FALSE END as less_than_25_orders

        FROM couriers
        LEFT JOIN orders ON orders.courier_id = couriers.id

        WHERE (CAST(to_char(couriers.first_order_time_local, 'YYMMDD') AS INT) >=
                   CAST(to_char(getdate() - INTERVAL '11 DAY', 'YYMMDD') AS INT))
                   AND (CAST(to_char(couriers.first_order_time_local, 'YYMMDD') AS INT) <=
                   CAST(to_char(getdate() - INTERVAL '7 DAY', 'YYMMDD') AS INT))
                AND couriers.staff = 0

        GROUP BY 1, 2, 3
    )

    SELECT courier_data.city_code,
           geography.country_code,
           courier_data.courier_id,
           courier_data.first_order_date,
           private_courier.courier_candidates.name,
           private_courier.courier_candidates.phone,
           courier_data.less_than_25_orders

    FROM courier_data
    LEFT JOIN private_courier.courier_candidates ON private_courier.courier_candidates.courier_id = courier_data.courier_id
    LEFT JOIN geography ON geography.code = courier_data.city_code

    WHERE geography.country_code IN ('RO','MA','UA','GE','KE','CI','HR','RS','KZ')
          AND less_than_25_orders IS TRUE

    ORDER BY first_order_date DESC