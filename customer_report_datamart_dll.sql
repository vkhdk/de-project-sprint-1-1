CREATE TABLE dwh.customer_report_datamart (
	id int8 GENERATED ALWAYS AS IDENTITY NOT NULL,
    customer_id int8 NOT NULL,
    customer_name varchar NOT NULL,
    customer_address varchar NOT NULL,
    customer_birthday date NOT NULL,
    customer_email varchar NOT NULL,
    customer_ltv numeric(15,2) NOT NULL DEFAULT 0, -- Если клиент не совершал заказов или нет завершенных заказов, то LTV = 0
    platform_income numeric(15,2) NOT NULL DEFAULT 0, -- Если клиент не совершал заказов, то платформа не заработала на нем
    count_month_orders int8 NOT NULL DEFAULT 0, -- Если клиент не совершал заказов в течении месяца, то количество заказов = 0
    avg_month_price_order numeric(10,2) NOT NULL DEFAULT 0, -- Если клиент не совершал заказов в течении месяца, то средняя стоимость заказа = 0
    median_month_order_lead_time_days int2 NOT NULL DEFAULT 0, -- Количество дней целое значение. Если клиент не совершал заказов в течении месяца, то медианное время выполнения заказа = 0
    top_month_product_category varchar NOT NULL DEFAULT 'No orders', -- Если клиент не совершал заказов в течении месяца, то категория продукта = 'No orders'
    top_craftsman_id varchar NOT NULL DEFAULT 'No orders', -- Если клиент не совершал заказов в течении месяца, то мастер = 'No orders'
    count_month_order_created int8 NOT NULL DEFAULT 0, -- Если клиенты не совершали заказов в течении месяца, то количество созданных заказов = 0
	count_month_order_in_progress int8 NOT NULL DEFAULT 0, -- Если клиенты не совершали заказов в течении месяца, то количество заказов в процессе выполнения = 0
	count_month_order_delivery int8 NOT NULL DEFAULT 0, -- Если клиенты не совершали заказов в течении месяца, то количество заказов на доставке = 0
	count_month_order_done int8 NOT NULL DEFAULT 0, -- Если клиенты не совершали заказов в течении месяца, то количество завершенных заказов = 0
	count_month_order_not_done int8 NOT NULL DEFAULT 0, -- Если клиенты не совершали заказов в течении месяца, то количество не завершенных заказов = 0
    report_period date NOT NULL,
    CONSTRAINT customer_report_datamart_pk PRIMARY KEY (id)
);