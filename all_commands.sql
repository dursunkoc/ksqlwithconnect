CREATE SOURCE CONNECTOR `mysql-connector` WITH(
    "connector.class"= 'io.debezium.connector.mysql.MySqlConnector',
    "tasks.max"= '1',
    "database.hostname"= 'mysql',
    "database.port"= '3306',
    "database.user"= 'root',
    "database.password"= 'debezium',
    "database.server.id"= '184054',
    "database.server.name"= 'dbserver1',
    "database.whitelist"= 'inventory',
    "table.whitelist"= 'inventory.customers,inventory.products,inventory.orders',
    "database.history.kafka.bootstrap.servers"= 'kafka:9092',
    "database.history.kafka.topic"= 'schema-changes.inventory',
    "transforms"= 'unwrap',
    "transforms.unwrap.type"= 'io.debezium.transforms.ExtractNewRecordState',
    "key.converter"= 'org.apache.kafka.connect.json.JsonConverter',
    "key.converter.schemas.enable"= 'false',
    "value.converter"= 'org.apache.kafka.connect.json.JsonConverter',
    "value.converter.schemas.enable"= 'false');

show topics;

SET 'auto.offset.reset' = 'earliest';

PRINT "dbserver1.inventory.customers" FROM BEGINNING;

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

select o.order_number, o.quantity, p.name as product from s_order as o left join t_product as p on p.id = o.product_id emit changes;

   select o.order_number, o.quantity, p.name as product, c.email as customer, p.id as product_id, c.id as customer_id
     from s_order as o 
left join t_product as p on o.product_id = p.id
left join t_customer as c on o.purchaser = c.id
emit changes;

CREATE STREAM SA_ENRICHED_ORDER WITH (VALUE_FORMAT='AVRO') AS
   select o.order_number, o.quantity, p.name as product, c.email as customer, p.id as product_id, c.id as customer_id
     from s_order as o 
left join t_product as p on o.product_id = p.id
left join t_customer as c on o.purchaser = c.id
partition by o.order_number
emit changes;

CREATE SINK CONNECTOR `postgres-sink` WITH(
    "connector.class"= 'io.confluent.connect.jdbc.JdbcSinkConnector',
    "tasks.max"= '1',
    "dialect.name"= 'PostgreSqlDatabaseDialect',
    "table.name.format"= 'ENRICHED_ORDER',
    "topics"= 'SA_ENRICHED_ORDER',
    "connection.url"= 'jdbc:postgresql://postgres:5432/inventory?user=postgresuser&password=postgrespw',
    "auto.create"= 'true',
    "insert.mode"= 'upsert',
    "pk.fields"= 'ORDER_NUMBER',
    "pk.mode"= 'record_key',
    "key.converter"= 'org.apache.kafka.connect.converters.IntegerConverter',
    "key.converter.schemas.enable" = 'false',
    "value.converter"= 'io.confluent.connect.avro.AvroConverter',
    "value.converter.schemas.enable" = 'true',
    "value.converter.schema.registry.url"= 'http://schema-registry:8081'
);