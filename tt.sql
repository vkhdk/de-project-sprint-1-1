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
top_product_type_month as (
select dd.customer_id, dd.product_type, dd.report_period, count(*),
ROW_NUMBER() OVER (PARTITION by dd.customer_id, report_period ORDER BY count(*) DESC) AS rn
from dwh_delta as dd
group by dd.customer_id, dd.product_type, dd.report_period),
top_craftsman_all as (
SELECT dd.customer_id, 
dd.craftsman_id, 
count(*), 
ROW_NUMBER() OVER (PARTITION by dd.customer_id ORDER BY count(*) DESC) AS rn
FROM dwh_delta as dd
GROUP BY dd.customer_id, dd.craftsman_id),
insert_sub_t as (
select 
dd.customer_id, 
dd.customer_name, 
tpm.product_type as top_month_product_category,
tc.craftsman_id as top_craftsman_id,
SUM(CASE WHEN dd.order_status = 'created' THEN 1 ELSE 0 END) AS count_month_order_created,
SUM(CASE WHEN dd.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_month_order_in_progress,
SUM(CASE WHEN dd.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_month_order_delivery, 
SUM(CASE WHEN dd.order_status = 'done' THEN 1 ELSE 0 END) AS count_month_order_done, 
SUM(CASE WHEN dd.order_status != 'done' THEN 1 ELSE 0 END) AS count_month_order_not_done,
dd.report_period
from dwh_delta as dd
left join top_craftsman_all as tc on 
dd.customer_id = tc.customer_id
and tc.rn = 1
left join top_product_type_month as tpm on dd.customer_id = tpm.customer_id and dd.report_period = tpm.report_period and tpm.rn = 1
group by dd.customer_id, dd.customer_name, dd.report_period,tc.craftsman_id, tpm.product_type)
select *
from insert_sub_t