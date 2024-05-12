WITH
dwh_delta AS ( -- определяем, какие данные были изменены в витрине или добавлены в DWH. Формируем дельту изменений
    SELECT     
            dcs.customer_id AS customer_id,
            dcs.customer_name AS customer_name,
            dcs.customer_address AS customer_address,
            dcs.customer_birthday AS customer_birthday,
            dcs.customer_email AS customer_email,
            dc.craftsman_id AS craftsman_id,
            fo.order_id AS order_id,
            dp.product_id AS product_id,
            dp.product_price AS product_price,
            dp.product_type AS product_type,
            fo.order_completion_date  AS order_completion_date,
            fo.order_created_date AS order_created_date,
            case when fo.order_completion_date is not null and fo.order_completion_date > fo.order_created_date then
            fo.order_completion_date - fo.order_created_date else null end as diff_order_date, -- не учитываем заказы, у которых дата завершения меньше даты создания
            fo.order_status AS order_status,
            date_trunc('month', fo.order_created_date)::date AS report_period,
            crd.customer_id AS exist_customer_id,
            dc.load_dttm AS craftsman_load_dttm,
            dcs.load_dttm AS customers_load_dttm,
            dp.load_dttm AS products_load_dttm
            FROM dwh.f_order fo 
                INNER JOIN dwh.d_craftsman dc ON fo.craftsman_id = dc.craftsman_id 
                INNER JOIN dwh.d_customer dcs ON fo.customer_id = dcs.customer_id 
                INNER JOIN dwh.d_product dp ON fo.product_id = dp.product_id 
                LEFT JOIN dwh.customer_report_datamart crd ON dcs.customer_id = crd.customer_id
                    WHERE (fo.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
                            (dc.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
                            (dcs.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
                            (dp.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart))
),
top_product_type_new_customer_month as ( -- самая популярная категория товаров у нового клиента за месяц
select dd.customer_id, dd.product_type, dd.report_period, count(*),
ROW_NUMBER() OVER (PARTITION by dd.customer_id, report_period ORDER BY count(*) DESC) AS rn
from dwh_delta as dd
WHERE dd.exist_customer_id IS NULL
group by dd.customer_id, dd.product_type, dd.report_period),
top_craftsman_new_customer_all_period as ( -- самый популярный мастер у нового клиента за весь период
SELECT dd.customer_id, 
dd.craftsman_id, 
count(*), 
ROW_NUMBER() OVER (PARTITION by dd.customer_id ORDER BY count(*) DESC) AS rn
FROM dwh_delta as dd
WHERE dd.exist_customer_id IS NULL
GROUP BY dd.customer_id, dd.craftsman_id),
new_customer_all_period_ltv as ( -- общая сумма, которую потратил новый клиент за весь период
SELECT dd.customer_id, 
SUM(dd.product_price) as customer_ltv
FROM dwh_delta as dd
WHERE dd.exist_customer_id IS NULL
GROUP BY dd.customer_id),
new_customers as ( -- формируем метрики по клиентам, которых ранее не было в витрине
select 
dd.customer_id, 
dd.customer_name, 
dd.customer_address,
dd.customer_birthday,
dd.customer_email,
SUM(dd.product_price)*0.10 as platform_income_month,
COUNT(dd.order_id) as count_month_orders,
AVG(dd.product_price) AS avg_month_price_order,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dd.diff_order_date)::int as median_month_order_lead_time_days,
SUM(CASE WHEN dd.order_status = 'created' THEN 1 ELSE 0 END) AS count_month_order_created,
SUM(CASE WHEN dd.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_month_order_in_progress,
SUM(CASE WHEN dd.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_month_order_delivery, 
SUM(CASE WHEN dd.order_status = 'done' THEN 1 ELSE 0 END) AS count_month_order_done, 
SUM(CASE WHEN dd.order_status != 'done' THEN 1 ELSE 0 END) AS count_month_order_not_done,
dd.report_period
from dwh_delta as dd
WHERE dd.exist_customer_id IS NULL -- выбираем только тех клиентов, которых ранее не было в витрине
group by dd.customer_id, dd.customer_name, dd.customer_address,dd.customer_birthday,dd.customer_email, dd.report_period),
insert_new_customers as(
select 
nc.customer_id,
nc.customer_name,
nc.customer_address,
nc.customer_birthday,
nc.customer_email,
ltv.customer_ltv,
nc.platform_income_month,
nc.count_month_orders,
nc.avg_month_price_order,
nc.median_month_order_lead_time_days,
tpm.product_type as top_month_product_category,
tc.craftsman_id as top_craftsman_id,
nc.count_month_order_created,
nc.count_month_order_in_progress,
nc.count_month_order_delivery,
nc.count_month_order_done,
nc.count_month_order_not_done,
nc.report_period
from new_customers as nc
left join top_craftsman_new_customer_all_period as tc on 
nc.customer_id = tc.customer_id
and tc.rn = 1
left join top_product_type_new_customer_month as tpm on nc.customer_id = tpm.customer_id 
and nc.report_period = tpm.report_period 
and tpm.rn = 1
left join new_customer_all_period_ltv as ltv on nc.customer_id = ltv.customer_id)
select * from insert_new_customers