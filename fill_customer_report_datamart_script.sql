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
        CASE 
            WHEN fo.order_completion_date IS NOT NULL 
                AND fo.order_completion_date > fo.order_created_date 
            THEN fo.order_completion_date - fo.order_created_date 
            ELSE NULL 
        END AS diff_order_date, -- не учитываем заказы, у которых дата завершения меньше даты создания
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
top_product_type_new_customer_month AS ( -- самая популярная категория товаров у нового клиента за месяц
    SELECT 
        dd.customer_id, 
        dd.product_type, 
        dd.report_period, 
        COUNT(*),
        ROW_NUMBER() OVER (PARTITION by dd.customer_id, report_period ORDER BY count(*) DESC) AS rn
    FROM dwh_delta AS dd
    WHERE dd.exist_customer_id IS NULL
    GROUP BY dd.customer_id, dd.product_type, dd.report_period),
top_craftsman_new_customer_all_period AS ( -- самый популярный мастер у нового клиента за весь период
    SELECT 
        dd.customer_id, 
        dd.craftsman_id, 
        COUNT(*), 
        ROW_NUMBER() OVER (PARTITION by dd.customer_id ORDER BY count(*) DESC) AS rn
    FROM dwh_delta as dd
    WHERE dd.exist_customer_id IS NULL
    GROUP BY dd.customer_id, dd.craftsman_id),
new_customer_all_period_ltv AS ( -- общая сумма, которую потратил новый клиент за весь период
    SELECT 
        dd.customer_id, 
        SUM(dd.product_price) as customer_ltv
    FROM dwh_delta as dd
    WHERE dd.exist_customer_id IS NULL
    GROUP BY dd.customer_id),
new_customers as ( -- формируем метрики по клиентам, которых ранее не было в витрине
    SELECT 
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
    FROM dwh_delta AS dd
    WHERE dd.exist_customer_id IS NULL -- выбираем только тех клиентов, которых ранее не было в витрине
    GROUP BY dd.customer_id, dd.customer_name, dd.customer_address,dd.customer_birthday,dd.customer_email, dd.report_period),
insert_new_customers AS ( -- таблица с новыми клиентами для вставки в витрину
    INSERT INTO dwh.customer_report_datamart (
        customer_id,
        customer_name,
        customer_address,
        customer_birthday,
        customer_email,
        customer_ltv,
        platform_income_month,
        count_month_orders,
        avg_month_price_order,
        median_month_order_lead_time_days,
        top_month_product_category,
        top_craftsman_id,
        count_month_order_created,
        count_month_order_in_progress,
        count_month_order_delivery,
        count_month_order_done,
        count_month_order_not_done,
        report_period)
    SELECT 
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
        tpm.product_type AS top_month_product_category,
        tc.craftsman_id AS top_craftsman_id,
        nc.count_month_order_created,
        nc.count_month_order_in_progress,
        nc.count_month_order_delivery,
        nc.count_month_order_done,
        nc.count_month_order_not_done,
        nc.report_period
FROM new_customers AS nc
    LEFT JOIN top_craftsman_new_customer_all_period AS tc 
        ON nc.customer_id = tc.customer_id
        AND tc.rn = 1
    LEFT JOIN top_product_type_new_customer_month AS tpm 
        ON nc.customer_id = tpm.customer_id 
        AND nc.report_period = tpm.report_period 
        AND tpm.rn = 1
    LEFT JOIN new_customer_all_period_ltv AS ltv 
        ON nc.customer_id = ltv.customer_id),
old_customers_id AS ( -- идентификаторы клиентов, которые уже были в витрине
    SELECT 
        dd.customer_id
    FROM dwh_delta AS dd
    WHERE dd.exist_customer_id IS NOT NULL),
dwh_old_customers AS ( -- получаем данные по клиентам, которые уже были в витрине
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
        CASE 
            WHEN fo.order_completion_date IS NOT NULL 
                AND fo.order_completion_date > fo.order_created_date 
            THEN fo.order_completion_date - fo.order_created_date 
            ELSE NULL 
        END AS diff_order_date, -- не учитываем заказы, у которых дата завершения меньше даты создания
        fo.order_status AS order_status,
        date_trunc('month', fo.order_created_date)::date AS report_period,
        dc.load_dttm AS craftsman_load_dttm,
        dcs.load_dttm AS customers_load_dttm,
        dp.load_dttm AS products_load_dttm
    FROM dwh.f_order fo 
        INNER JOIN dwh.d_craftsman dc ON fo.craftsman_id = dc.craftsman_id 
        INNER JOIN dwh.d_customer dcs ON fo.customer_id = dcs.customer_id 
        INNER JOIN dwh.d_product dp ON fo.product_id = dp.product_id 
        INNER JOIN old_customers_id ON fo.customer_id = old_customers_id.customer_id),
top_product_type_old_customer_month AS ( -- самая популярная категория товаров у старых клиентов за месяц
    SELECT 
        doc.customer_id, 
        doc.product_type, 
        doc.report_period, 
        COUNT(*),
        ROW_NUMBER() OVER (PARTITION by doc.customer_id, doc.report_period ORDER BY count(*) DESC) AS rn
    FROM dwh_old_customers AS doc
    GROUP BY doc.customer_id, doc.product_type, doc.report_period),
top_craftsman_old_customer_all_period AS ( -- самый популярный мастер у старых клиентов за весь период
    SELECT 
        doc.customer_id, 
        doc.craftsman_id, 
        COUNT(*), 
        ROW_NUMBER() OVER (PARTITION by doc.customer_id ORDER BY count(*) DESC) AS rn
    FROM dwh_old_customers AS doc
    GROUP BY doc.customer_id, doc.craftsman_id),
old_customer_all_period_ltv AS ( -- общая сумма, которую потратил старый клиент за весь период
    SELECT 
        doc.customer_id, 
        SUM(doc.product_price) as customer_ltv
    FROM dwh_old_customers AS doc
    GROUP BY doc.customer_id),
old_customers as ( -- формируем метрики по клиентам, которыу уже были в витрине
    SELECT 
        doc.customer_id, 
        doc.customer_name, 
        doc.customer_address,
        doc.customer_birthday,
        doc.customer_email,
        SUM(doc.product_price)*0.10 as platform_income_month,
        COUNT(doc.order_id) as count_month_orders,
        AVG(doc.product_price) AS avg_month_price_order,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY doc.diff_order_date)::int as median_month_order_lead_time_days,
        SUM(CASE WHEN doc.order_status = 'created' THEN 1 ELSE 0 END) AS count_month_order_created,
        SUM(CASE WHEN doc.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_month_order_in_progress,
        SUM(CASE WHEN doc.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_month_order_delivery, 
        SUM(CASE WHEN doc.order_status = 'done' THEN 1 ELSE 0 END) AS count_month_order_done, 
        SUM(CASE WHEN doc.order_status != 'done' THEN 1 ELSE 0 END) AS count_month_order_not_done,
        doc.report_period
    FROM dwh_old_customers AS doc
    GROUP BY doc.customer_id, doc.customer_name, doc.customer_address,doc.customer_birthday,doc.customer_email, doc.report_period),
insert_old_customers AS ( -- таблица со старыми клиентами для вставки в витрину
    UPDATE dwh.customer_report_datamart SET
        customer_id = updates.customer_id,
        customer_name = updates.customer_name,
        customer_address = updates.customer_address,
        customer_birthday = updates.customer_birthday,
        customer_email = updates.customer_email,
        customer_ltv = updates.customer_ltv,
        platform_income_month = updates.platform_income_month,
        count_month_orders = updates.count_month_orders,
        avg_month_price_order = updates.avg_month_price_order,
        median_month_order_lead_time_days = updates.median_month_order_lead_time_days,
        top_month_product_category = updates.top_month_product_category,
        top_craftsman_id = updates.top_craftsman_id,
        count_month_order_created = updates.count_month_order_created,
        count_month_order_in_progress = updates.count_month_order_in_progress,
        count_month_order_delivery = updates.count_month_order_delivery,
        count_month_order_done = updates.count_month_order_done,
        count_month_order_not_done = updates.count_month_order_not_done,
        report_period = updates.report_period  
    FROM (
    SELECT 
        oc.customer_id,
        oc.customer_name,
        oc.customer_address,
        oc.customer_birthday,
        oc.customer_email,
        ltv.customer_ltv,
        oc.platform_income_month,
        oc.count_month_orders,
        oc.avg_month_price_order,
        oc.median_month_order_lead_time_days,
        tpm.product_type AS top_month_product_category,
        tc.craftsman_id AS top_craftsman_id,
        oc.count_month_order_created,
        oc.count_month_order_in_progress,
        oc.count_month_order_delivery,
        oc.count_month_order_done,
        oc.count_month_order_not_done,
        oc.report_period
    FROM old_customers AS oc
    LEFT JOIN top_craftsman_old_customer_all_period AS tc 
        ON oc.customer_id = tc.customer_id
        AND tc.rn = 1
    LEFT JOIN top_product_type_old_customer_month AS tpm 
        ON oc.customer_id = tpm.customer_id 
        AND oc.report_period = tpm.report_period 
        AND tpm.rn = 1
    LEFT JOIN old_customer_all_period_ltv AS ltv 
        ON oc.customer_id = ltv.customer_id) AS updates),
insert_load_date AS ( -- делаем запись в таблицу загрузок о том, когда была совершена загрузка, чтобы в следующий раз взять данные, которые будут добавлены или изменены после этой даты
    INSERT INTO dwh.load_dates_customer_report_datamart (
        load_dttm
    )
    SELECT GREATEST(COALESCE(MAX(craftsman_load_dttm), NOW()), 
                    COALESCE(MAX(customers_load_dttm), NOW()), 
                    COALESCE(MAX(products_load_dttm), NOW())) 
        FROM dwh_delta
)
SELECT 'increment datamart'; -- инициализируем запрос CTE