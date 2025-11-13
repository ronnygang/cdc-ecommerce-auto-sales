# Arquitectura CDC Pipeline - Ecommerce de Autos

## 📐 Diseño del Sistema

### Arquitectura de Alto Nivel

```
┌─────────────────────────────────────────────────────────────────┐
│                    ECOMMERCE CDC PIPELINE                        │
└─────────────────────────────────────────────────────────────────┘

┌──────────────────┐        ┌──────────────────┐        ┌──────────────────┐
│   PostgreSQL     │        │   Apache Kafka   │        │   ClickHouse     │
│   (OLTP Source)  │───────▶│  (Event Stream)  │───────▶│  (OLAP Target)   │
└──────────────────┘        └──────────────────┘        └──────────────────┘
         │                           │                            │
         │                           │                            │
    Transactions              CDC Events                    Analytics
    (Real-time)              (Streaming)                    (Real-time)
         │                           │                            │
         ▼                           ▼                            ▼
  ┌────────────┐            ┌────────────┐              ┌────────────┐
  │  Debezium  │            │  Consumer  │              │Materialized│
  │  Connector │            │  (Python)  │              │   Views    │
  └────────────┘            └────────────┘              └────────────┘
```

## 🔄 Flujo de Datos

### 1. Captura de Cambios (CDC)

```
PostgreSQL WAL ──▶ Debezium ──▶ Kafka Topics
                      │
                      ├─ CREATE (INSERT)
                      ├─ UPDATE
                      └─ DELETE
```

**Debezium Configuration:**
- Plugin: `pgoutput` (native PostgreSQL logical replication)
- Snapshot Mode: `initial` (captura estado inicial)
- Table Filtering: Solo tablas relevantes del ecommerce
- Heartbeat: Cada 10 segundos

### 2. Streaming y Procesamiento

```
Kafka Topics
    │
    ├─ cdc.ecommerce.*.customers
    ├─ cdc.ecommerce.*.vehicles
    ├─ cdc.ecommerce.*.orders
    ├─ cdc.ecommerce.*.order_items
    └─ cdc.ecommerce.*.payments
    
    ↓
    
Python Consumer
    │
    ├─ Deserialización JSON
    ├─ Transformación de datos
    ├─ Enriquecimiento con metadata
    └─ Batch processing
    
    ↓
    
ClickHouse Loader
```

### 3. Almacenamiento Analítico

```
ClickHouse Tables
    │
    ├─ ReplacingMergeTree Engine
    │  └─ Maneja UPDATE/DELETE automáticamente
    │
    ├─ Partitioning por fecha
    │  └─ Optimiza queries temporales
    │
    └─ Materialized Views
       ├─ Daily Sales Summary
       ├─ Vehicle Inventory
       ├─ Customer Lifetime Value
       └─ Payment Analytics
```

## 🏗️ Componentes del Sistema

### PostgreSQL (Source Database)

**Propósito:** Base de datos transaccional OLTP

**Tablas:**
- `customers` - Información de clientes
- `vehicles` - Inventario de vehículos
- `orders` - Órdenes de compra
- `order_items` - Detalle de items
- `payments` - Transacciones de pago

**Configuraciones Clave:**
```sql
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
```

### Debezium CDC Connector

**Propósito:** Captura de cambios en tiempo real

**Características:**
- Captura eventos INSERT, UPDATE, DELETE
- Snapshot inicial de datos existentes
- Fault tolerance con replication slots
- Transformaciones inline (unwrap)

### Apache Kafka

**Propósito:** Event streaming backbone

**Componentes:**
- **Kafka Broker:** Almacenamiento distribuido de eventos
- **Zookeeper:** Coordinación del cluster
- **Schema Registry:** Gestión de schemas
- **Kafka Connect:** Framework para connectors

**Configuración:**
- Replication Factor: 1 (desarrollo)
- Partitions: 3 por topic
- Retention: 7 días

### Python Consumer

**Propósito:** Procesamiento y transformación de eventos

**Funcionalidades:**
- Consumo de múltiples topics simultáneamente
- Batch processing para eficiencia
- Transformación de datos CDC a formato ClickHouse
- Error handling y retries
- Logging estructurado

**Características Técnicas:**
- Confluent Kafka client (librdkafka)
- Procesamiento asíncrono
- Commit manual de offsets
- Graceful shutdown

### ClickHouse (Target Database)

**Propósito:** Base de datos analítica OLAP

**Engine:** ReplacingMergeTree
- Maneja automáticamente duplicados
- Usa `updated_at` como versión
- OPTIMIZE FINAL para merges

**Optimizaciones:**
- Índices columnares
- Particionamiento por mes
- Materialized views para agregaciones
- Compresión eficiente

## 📊 Modelo de Datos

### Schema Evolution

```
PostgreSQL (Normalized)
    ↓
Debezium (CDC Events)
    ↓
Python (Transformation)
    ↓
ClickHouse (Denormalized + Metadata)
```

### Metadata Adicional en ClickHouse

Cada tabla incluye:
```sql
cdc_operation     String      -- INSERT/UPDATE/DELETE
cdc_timestamp     DateTime64  -- Timestamp del cambio
cdc_source_db     String      -- Database origen
cdc_source_table  String      -- Tabla origen
event_time        DateTime    -- Timestamp de procesamiento
```

## 🔐 Seguridad y Confiabilidad

### Replication Slots
- Garantiza no pérdida de eventos
- Mantiene estado de consumo
- Recovery automático

### Offset Management
- Commits manuales después de escribir a ClickHouse
- Exactly-once semantics con transacciones

### Error Handling
- Retries exponenciales
- Dead letter queue (futuro)
- Logging completo de errores

## 📈 Escalabilidad

### Horizontal Scaling

**Kafka:**
- Agregar más brokers
- Incrementar particiones por topic

**Python Consumers:**
- Múltiples instancias del consumer
- Cada instancia procesa diferentes particiones

**ClickHouse:**
- Sharding por hash de customer_id
- Distributed tables
- Replication para HA

### Performance Optimization

**Batch Processing:**
- Consumer: Batch de 100 mensajes
- ClickHouse: Inserts en bloque

**Partitioning:**
- Por mes en ClickHouse
- Pruning automático de particiones

**Materialized Views:**
- Pre-agregaciones
- Queries sub-segundo

## 🔍 Monitoreo

### Métricas Clave

**Debezium:**
- Snapshot status
- Binlog position
- Event lag

**Kafka:**
- Consumer lag
- Throughput
- Partition distribution

**ClickHouse:**
- Insert rate
- Query performance
- Storage size

### Herramientas

- **Kafka UI:** Visualización de topics y consumer groups
- **Logs estructurados:** JSON logging con structlog
- **Health checks:** Script de validación de componentes

## 🎯 Casos de Uso

### 1. Dashboard de Ventas en Tiempo Real
```sql
SELECT 
    toDate(order_date) as date,
    count() as orders,
    sum(final_amount) as revenue
FROM orders
WHERE order_date >= now() - INTERVAL 7 DAY
GROUP BY date
```

### 2. Análisis de Inventario
```sql
SELECT 
    make, model, status,
    count() as count,
    avg(price) as avg_price
FROM vehicles
GROUP BY make, model, status
```

### 3. Customer Lifetime Value
```sql
SELECT 
    c.customer_id,
    c.email,
    count(o.order_id) as total_orders,
    sum(o.final_amount) as lifetime_value
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.email
ORDER BY lifetime_value DESC
LIMIT 100
```

## 🚀 Próximos Pasos

### Mejoras Futuras

1. **Stream Processing con Apache Flink**
   - Complex event processing
   - Windowing avanzado
   - Stateful transformations

2. **Data Quality Monitoring**
   - Great Expectations
   - Anomaly detection
   - Data lineage

3. **Real-time Dashboards**
   - Grafana/Superset
   - WebSocket APIs
   - Streaming visualizations

4. **Machine Learning Integration**
   - Predictive analytics
   - Recommendation engine
   - Fraud detection

5. **Multi-region Replication**
   - Kafka MirrorMaker
   - ClickHouse replication
   - Geo-distribution
