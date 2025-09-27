# Reto 2 – Plataforma de streaming con Kafka, ksqlDB, Connect, Postgres y Grafana

Plataforma de streaming que simula órdenes de compra, las procesa con **ksqlDB**, las persiste en **PostgreSQL** mediante **Kafka Connect JDBC**, y las visualiza con **Grafana**. La operación y monitoreo se apoya en **Kafka UI**.

---

##  Componentes

Servicios orquestados en `docker-compose.yml`:

- **Kafka** (`bitnami/kafka:3.7`) – Broker único (KRaft).  
  Listeners: `PLAINTEXT://kafka:9092` (red Docker) y `PLAINTEXT_HOST://localhost:29092` (host).
- **Kafka UI** (`provectuslabs/kafka-ui:latest`) – Explorador web.  
  `http://localhost:8080`
- **Schema Registry** (`confluentinc/cp-schema-registry:7.6.1`) – Gestión de esquemas Avro.  
  `http://localhost:8081`
- **ksqlDB Server** (`confluentinc/ksqldb-server:0.29.0`) – SQL sobre streams.  
  `http://localhost:8088`
- **ksqlDB CLI** (`confluentinc/ksqldb-cli:0.29.0`) – Cliente interactivo.
- **Kafka Connect** (`confluentinc/cp-kafka-connect:7.6.1`) – Marco de conectores.  
  REST: `http://localhost:8083`
- **PostgreSQL** (`postgres:15`) – Base **streaming** (`postgres/postgres`).  
  `localhost:5432`
- **Grafana** (`grafana/grafana:10.4.0`) – Dashboards.  
  `http://localhost:3000` (admin/admin)

Código de aplicación:

- **Producer** (`producer/producer.py`) – Emite eventos **JSON** al tópico `orders`.
- **ksqlDB** (`ksql/queries.sql`) – Define:
  - `ORDERS_RAW` (stream JSON desde `orders`)
  - `ORDERS_ENR_AVRO` (stream enriquecido a **Avro**, topic `orders_enriched_avro`)
  - `ALERTS_AVRO` (stream de alertas en **Avro**, topic `alerts_avro`)
  - `SALES_PER_MIN_AVRO` (**tabla** con ventana de 1 min en **Avro**, **KEY_FORMAT JSON**; topic `sales_per_min_avro`)
- **Conectores JDBC** (`connectors/*.json`):
  - `orders-sink.json` → Postgres (tabla `orders_enriched_avro`)
  - `metrics-sink.json` → Postgres (tabla `sales_per_min_avro`, con DLQ)

> **Nota**: `SALES_PER_MIN_AVRO` usa **KEY_FORMAT='JSON'** porque la clave es compuesta (`product_id`, `product_name`). Evita el error “KAFKA format only supports a single field”.

---

##  Requisitos

- Docker y Docker Compose
- Python 3.9+ (solo para ejecutar el producer)
- `curl` (para registrar conectores)

---

##  Puesta en marcha

### 1) Levantar el stack

```bash
docker compose up -d
```

Endpoints de verificación:

- Kafka UI: `http://localhost:8080`
- Schema Registry: `http://localhost:8081/subjects`
- ksqlDB: `http://localhost:8088/info`
- Kafka Connect: `http://localhost:8083/`  (y `/connector-plugins`)
- Grafana: `http://localhost:3000`  (admin/admin)

### 2) Instalar plugin JDBC en **Connect** (1ª vez)

```bash
# entrar al contenedor
docker compose exec -it connect bash
# instalar JDBC
confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest
exit
# reiniciar Connect para que cargue el plugin
docker compose restart connect
```

Verifica que aparezca el plugin:

```bash
curl -s http://localhost:8083/connector-plugins | jq .
```

Debe listar `io.confluent.connect.jdbc.JdbcSinkConnector`.

### 3) Crear artefactos de **ksqlDB**

Copia el script y ejecútalo:

```bash
docker cp ksql/queries.sql ksqldb-cli:/tmp/queries.sql
docker exec -it ksqldb-cli ksql http://ksqldb:8088 -e "RUN SCRIPT '/tmp/queries.sql';"
```

Verifica:

```bash
docker exec -it ksqldb-cli ksql http://ksqldb:8088
# ya en el CLI:
SHOW STREAMS;
SHOW TABLES;
SHOW QUERIES;
```

Deberías ver:
- Streams: `ORDERS_RAW`, `ORDERS_ENR_AVRO`, `ALERTS_AVRO`
- Tabla: `SALES_PER_MIN_AVRO`
- 3 queries persistentes **RUNNING**

### 4) Correr el **producer**

**Windows (CMD):**
```cmd
python -m venv .venv
.\.venv\Scripts\activate
pip install confluent-kafka
python producer\producer.py
```

**Linux/macOS:**
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install confluent-kafka
python producer/producer.py
```

### 5) Registrar conectores **JDBC Sink**

Crea los archivos en `connectors/`:

**connectors/orders-sink.json**
```json
{
  "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
  "topics": "orders_enriched_avro",
  "connection.url": "jdbc:postgresql://postgres:5432/streaming",
  "connection.user": "postgres",
  "connection.password": "postgres",
  "auto.create": "true",
  "auto.evolve": "true",
  "insert.mode": "insert"
}
```

**connectors/metrics-sink.json**  
*(incluye ajustes para clave JSON, Avro en value y DLQ con RF=1)*

```json
{
  "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
  "topics": "sales_per_min_avro",
  "connection.url": "jdbc:postgresql://postgres:5432/streaming",
  "connection.user": "postgres",
  "connection.password": "postgres",
  "auto.create": "true",
  "insert.mode": "insert",

  "consumer.override.auto.offset.reset": "earliest",

  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter.schema.registry.url": "http://schema-registry:8081",

  "errors.tolerance": "all",
  "errors.log.enable": "true",
  "errors.deadletterqueue.topic.name": "dlq_sales_per_min_avro",
  "errors.deadletterqueue.context.headers.enable": "true",
  "errors.deadletterqueue.topic.replication.factor": "1"
}
```

Regístralos:

```bash
curl -s -X PUT -H 'Content-Type: application/json' \
  --data @connectors/orders-sink.json \
  http://localhost:8083/connectors/postgres-sink-orders/config

curl -s -X PUT -H 'Content-Type: application/json' \
  --data @connectors/metrics-sink.json \
  http://localhost:8083/connectors/postgres-sink-metrics/config
```

Estado:

```bash
curl -s http://localhost:8083/connectors
curl -s http://localhost:8083/connectors/postgres-sink-orders/status | jq .
curl -s http://localhost:8083/connectors/postgres-sink-metrics/status | jq .
```

### 6) Validaciones rápidas

**Kafka UI (`:8080`)**
- Ver topics: `orders`, `orders_enriched_avro`, `alerts_avro`, `sales_per_min_avro`
- Abrir *Messages* y confirmar tráfico.

**PostgreSQL**
```bash
docker exec -it postgres psql -U postgres -d streaming -c "\dt"

# Últimas órdenes (¡OJO: columnas en MAYÚSCULAS requieren comillas!)
docker exec -it postgres psql -U postgres -d streaming -c \
"SELECT to_timestamp(\"TS_MILLIS\"/1000.0) AS time,
        \"CUSTOMER\", \"PRODUCT_NAME\", \"QUANTITY\", \"TOTAL\", \"INVENTORY_LEFT\"
 FROM orders_enriched_avro
 ORDER BY time DESC
 LIMIT 10;"

# Agregado por minuto
docker exec -it postgres psql -U postgres -d streaming -c \
"SELECT to_timestamp(\"WINDOW_START_MS\"/1000.0) AS window_start,
        \"UNITS\", \"REVENUE\"
 FROM sales_per_min_avro
 ORDER BY window_start DESC
 LIMIT 10;"
```

---

## 📊 Grafana – Data Source y Paneles

### 1) Data Source PostgreSQL
- **Type**: PostgreSQL
- **Host URL**: `postgres:5432`  *(usar el nombre del servicio, no localhost)*
- **Database**: `streaming`
- **Username/Password**: `postgres` / `postgres`
- **TLS/SSL Mode**: `disable`
- Test → **Database Connection OK**.

### 2) Dashboard (2 paneles)

#### Panel A – *“Órdenes recientes”* (Table)
```sql
SELECT
  to_timestamp("TS_MILLIS"/1000.0) AS time,
  "CUSTOMER", "PRODUCT_NAME", "QUANTITY", "TOTAL", "INVENTORY_LEFT"
FROM orders_enriched_avro
WHERE $__timeFilter(to_timestamp("TS_MILLIS"/1000.0))
ORDER BY time DESC
LIMIT 50;
```
- Visualization: **Table**
- Title sugerido: **Órdenes recientes**
- Time range: a gusto (p.ej. *Last 6 hours*)

#### Panel B – *“Revenue & Unidades por minuto”* (Time series)
```sql
SELECT
  to_timestamp("WINDOW_START_MS"/1000.0) AS time,
  SUM("UNITS")    AS units,
  SUM("REVENUE")  AS revenue
FROM sales_per_min_avro
WHERE $__timeFilter(to_timestamp("WINDOW_START_MS"/1000.0))
GROUP BY 1
ORDER BY 1;
```
- Visualization: **Time series**
- Series: `units` y `revenue` (dos líneas).  
  Puedes ajustar unidad y ejes (e.g., revenue en eje derecho).
- Title sugerido: **Revenue & Units por minuto**
- Refresh: cada **5s–10s** (arriba a la derecha).

> Guarda el dashboard: **Save** → nombre sugerido **Kafka**.  
> Export opcional para entregar: **Share** → **Export** → “Export for sharing externally”.

---

##  Observaciones importantes

- **Claves y columnas en JDBC Sink**: por defecto, el conector solo guarda **value**; la **key** (que en `sales_per_min_avro` contiene `product_id` y `product_name`) no se inserta como columnas.  
  Si necesitas esas columnas en Postgres, puedes usar:
  ```json
  {
    "pk.mode": "record_key",
    "pk.fields": "PRODUCT_ID,PRODUCT_NAME",
    "insert.mode": "upsert"
  }
  ```
  *(requerirá que `sales_per_min_avro` tenga la key serializable a columnas; en nuestro caso es JSON → añade `key.converter.schemas.enable=true` y un mapeo acorde, o crea un stream que materialice las keys en value).*

- **Nombres de campos en MAYÚSCULAS**: ksqlDB emitió Avro con nombres en mayúsculas; Postgres mantiene ese casing. Por eso, las consultas usan comillas (`"TS_MILLIS"`).

---

##  Comandos útiles

```bash
# Consumir algunos mensajes del topic 'orders' desde el broker
docker exec -it kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders --from-beginning --max-messages 5

# Listar conectores y su estado
curl -s http://localhost:8083/connectors | jq .
curl -s http://localhost:8083/connectors/postgres-sink-orders/status | jq .
curl -s http://localhost:8083/connectors/postgres-sink-metrics/status | jq .

# Limpiar todo (incluye volúmenes)
docker compose down -v
```

---

##  Troubleshooting (casos reales resueltos)

- **`Unknown magic byte!` al arrancar `metrics-sink`**  
  Causa: mismatch de converters (key JSON, value Avro).  
   Solución aplicada en `metrics-sink.json`:
  ```json
  {
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081"
  }
  ```

- **DLQ falla por `InvalidReplicationFactorException`**  
  Causa: DLQ por defecto intenta RF=3 y hay 1 broker.  
   Solución:
  ```json
  {
    "errors.deadletterqueue.topic.replication.factor": "1"
  }
  ```

- **Grafana “Validation error, invalid URL”**  
  Causa: usar `localhost:5432` desde Grafana (container ≠ host).  
   Solución: usar `postgres:5432`.

- **`KAFKA` key format solo 1 campo**  
  Causa: la clave compuesta de la tabla.  
   Solución aplicada: `KEY_FORMAT='JSON'` en `SALES_PER_MIN_AVRO`.

---

##  Estructura del repo

```
.
├── docker-compose.yml
├── ksql/
│   └── queries.sql
├── producer/
│   └── producer.py
├── orders-sink.json --> conector 
├── metrics-sink.json --> conector 
└── README.md  ← (este archivo)
```

---


