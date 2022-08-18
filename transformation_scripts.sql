CREATE STREAM S_CUSTOMER (ID INT,
                       FIRST_NAME string,
                       LAST_NAME string,
                       EMAIL string)
                 WITH (KAFKA_TOPIC='dbserver1.inventory.customers',
                       VALUE_FORMAT='json');

CREATE TABLE T_CUSTOMER
AS
    SELECT id,
           latest_by_offset(first_name) as fist_name,
           latest_by_offset(last_name) as last_name,
           latest_by_offset(email) as email
    FROM s_customer
    GROUP BY id
    EMIT CHANGES;

CREATE STREAM S_PRODUCT (ID INT,
                       NAME string,
                       description string,
                       weight DOUBLE)
                 WITH (KAFKA_TOPIC='dbserver1.inventory.products',
                       VALUE_FORMAT='json');

CREATE TABLE T_PRODUCT
AS
    SELECT id,
           latest_by_offset(name) as name,
           latest_by_offset(description) as description,
           latest_by_offset(weight) as weight
    FROM s_product
    GROUP BY id
    EMIT CHANGES;

CREATE STREAM s_order (
    order_number integer,
    order_date timestamp,
    purchaser integer,
    quantity integer,
    product_id integer) 
    WITH (KAFKA_TOPIC='dbserver1.inventory.orders',VALUE_FORMAT='json');

CREATE STREAM SA_ENRICHED_ORDER WITH (VALUE_FORMAT='AVRO') AS
   select o.order_number, o.quantity, p.name as product, c.email as customer, p.id as product_id, c.id as customer_id
     from s_order as o 
left join t_product as p on o.product_id = p.id
left join t_customer as c on o.purchaser = c.id
partition by o.order_number
emit changes;