-- 1) Stream de entrada desde el topic JSON 'orders'
CREATE STREAM ORDERS_RAW (
  order_id STRING,
  ts BIGINT,
  customer STRING,
  product_id STRING,
  product_name STRING,
  quantity INT,
  price DOUBLE,
  inventory_left INT
) WITH (
  KAFKA_TOPIC='orders',
  VALUE_FORMAT='JSON'
);

-- 2) Enriquecido a AVRO (para Connect) + timestamp en ms y en ISO
CREATE STREAM ORDERS_ENR_AVRO
    WITH (KAFKA_TOPIC='orders_enriched_avro', VALUE_FORMAT='AVRO') AS
SELECT
    order_id,
    ts AS ts_millis,
    TIMESTAMPTOSTRING(ts, 'yyyy-MM-dd''T''HH:mm:ss.SSSX') AS ts_iso,
    customer,
    product_id,
    product_name,
    quantity,
    price,
    quantity * price AS total,
    inventory_left
FROM ORDERS_RAW
EMIT CHANGES;

-- 3) Alertas por inventario bajo (AVRO)
CREATE STREAM ALERTS_AVRO
    WITH (KAFKA_TOPIC='alerts_avro', VALUE_FORMAT='AVRO') AS
SELECT
    order_id,
    product_id,
    product_name,
    inventory_left,
    ts AS ts_millis
FROM ORDERS_RAW
WHERE inventory_left <= 10
EMIT CHANGES;

-- 4) Métricas por minuto (tabla materializada en AVRO)
--    👇 clave compuesta => especificamos KEY_FORMAT que soporte schema
CREATE TABLE SALES_PER_MIN_AVRO
    WITH (KAFKA_TOPIC='sales_per_min_avro', VALUE_FORMAT='AVRO', KEY_FORMAT='JSON') AS
SELECT
    product_id,
    product_name,
    WINDOWSTART AS window_start_ms,
    SUM(quantity)       AS units,
    SUM(quantity*price) AS revenue
FROM ORDERS_RAW
WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY product_id, product_name
EMIT CHANGES;
