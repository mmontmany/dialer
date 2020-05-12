with tb_1 as (

SELECT
       c.id as courier_id,
       c.city_code,
       c.transport,
       c.excellence_score,
       c.last_order_time_local as courier_last_order,
       max(case when c.last_order_time_local < getdate() - interval '4 weeks' then 1 else 0 end) as churned_last_week,
       max(case when c.last_order_time_local < getdate() - interval '3 weeks' then 1 else 0 end) as no_orders_previous_3w,
       count(distinct case when ssc.start_time between getdate() AND getdate() + interval '1 week' then ssc.id END) as slots_next_week

FROM couriers c
LEFT JOIN scheduling_slots_couriers ssc on c.id = ssc.courier_id
WHERE c.staff=0 AND c.first_order_time_local IS NOT NULL AND c.last_order_time_local > getdate() - interval '5 weeks'
group by 1,2,3,4,5)

SELECT tb_1.city_code,
       g.country_code,
       tb_1.courier_id,
       private_courier.courier_candidates.name,
       private_courier.courier_candidates.phone,
       tb_1.excellence_score,
       1.0 * courier_wallets.cash_balance/100 AS cash_balance,
       tb_1.transport,
       tb_1.courier_last_order,
       tb_1.churned_last_week,
       tb_1.no_orders_previous_3w,
       tb_1.slots_next_week,
       case when tb_1.no_orders_previous_3w=1 AND tb_1.slots_next_week = 0 AND tb_1.churned_last_week=0 then 1 else 0 end as potential_churner,
       case when tb_1.slots_next_week = 0 AND tb_1.churned_last_week=1 then 1 else 0 end as churner


FROM tb_1
LEFT JOIN geography g ON tb_1.city_code = g.code
LEFT JOIN courier_wallets ON tb_1.courier_id = courier_wallets.courier_id
LEFT JOIN private_courier.courier_candidates ON tb_1.courier_id = private_courier.courier_candidates.courier_id

WHERE g.country_code IN ('RO','MA','UA','GE','KE','CI','HR','RS','KZ')
AND (potential_churner = 1 OR churner = 1)

ORDER BY  tb_1.churned_last_week DESC, tb_1.excellence_score DESC;

















