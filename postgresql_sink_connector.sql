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