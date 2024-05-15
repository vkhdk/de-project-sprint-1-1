DROP TABLE IF EXISTS dwh.customer_report_datamart;
CREATE TABLE dwh.customer_report_datamart (
	id int8 GENERATED ALWAYS AS IDENTITY NOT NULL, -- идентификатор записи
    customer_id int8 NOT NULL, -- идентификатор заказчика
    customer_name varchar NOT NULL, -- Ф. И. О. заказчика
    customer_address varchar NOT NULL, -- адрес заказчика
    customer_birthday date NOT NULL, -- дата рождения заказчика
    customer_email varchar NOT NULL, -- электронная почта заказчика

    customer_ltv numeric(15,2) NOT NULL DEFAULT 0, -- сумма, которую потратил заказчик за весь период - LTV. 
    --Если клиент не совершал заказов или нет завершенных заказов, то LTV = 0

    platform_income_month numeric(15,2) NOT NULL DEFAULT 0, -- сумма, которую заработала платформа от покупок заказчика за месяц (10% от суммы, которую потратил заказчик)
    --Если клиент не совершал заказов, то платформа не заработала на нем

    count_month_orders int8 NOT NULL DEFAULT 0, -- количество заказов у заказчика за месяц
    -- Если клиент не совершал заказов в течении месяца, то количество заказов = 0

    avg_month_price_order numeric(10,2) NOT NULL DEFAULT 0, -- средняя стоимость одного заказа у заказчика за месяц
    -- Если клиент не совершал заказов в течении месяца, то средняя стоимость заказа = 0

    median_month_order_lead_time_days int2 NULL, -- медианное время в днях от момента создания заказа до его завершения за месяц
    -- Количество дней целое значение. Если клиент не совершал заказов в течении месяца, то медианное время выполнения заказа = Null
    
    top_month_product_category varchar NOT NULL DEFAULT 'No orders', -- самая популярная категория товаров у этого заказчика за месяц
    -- Если клиент не совершал заказов в течении месяца, то категория продукта = 'No orders'

    top_craftsman_id varchar NOT NULL DEFAULT 'No orders', -- идентификатор самого популярного мастера ручной работы у заказчика. Если заказчик сделал одинаковое количество заказов у нескольких мастеров, возьмите любого
    -- Если клиент не совершал заказов в течении месяца, то мастер = 'No orders'

    count_month_order_created int8 NOT NULL DEFAULT 0, -- количество созданных заказов за месяц
    -- Если клиенты не совершали заказов в течении месяца, то количество созданных заказов = 0

	count_month_order_in_progress int8 NOT NULL DEFAULT 0, -- количество заказов в процессе изготовки за месяц
    -- Если клиенты не совершали заказов в течении месяца, то количество заказов в процессе выполнения = 0

	count_month_order_delivery int8 NOT NULL DEFAULT 0, -- количество заказов в доставке за месяц
    -- Если клиенты не совершали заказов в течении месяца, то количество заказов на доставке = 0

	count_month_order_done int8 NOT NULL DEFAULT 0, -- количество завершённых заказов за месяц
    -- Если клиенты не совершали заказов в течении месяца, то количество завершенных заказов = 0

	count_month_order_not_done int8 NOT NULL DEFAULT 0, -- количество незавершённых заказов за месяц
    -- Если клиенты не совершали заказов в течении месяца, то количество не завершенных заказов = 0

    report_period date NOT NULL, -- отчётный период, год и месяц
    CONSTRAINT customer_report_datamart_pk PRIMARY KEY (id)
);