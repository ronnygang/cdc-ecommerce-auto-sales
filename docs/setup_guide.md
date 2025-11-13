# Guía de Setup - CDC Pipeline Ecommerce

## 📋 Prerequisitos

### Software Requerido

- **Docker Desktop**: v20.10+
- **Docker Compose**: v2.0+
- **Python**: 3.9+
- **Git**: Para clonar el repositorio
- **curl**: Para ejecutar scripts de setup

### Recursos del Sistema

**Mínimo Recomendado:**
- CPU: 4 cores
- RAM: 8 GB
- Disk: 20 GB libres

**Óptimo:**
- CPU: 8+ cores
- RAM: 16 GB
- Disk: 50 GB SSD

## 🚀 Instalación Paso a Paso

### 1. Iniciar Servicios Docker

Navegar al directorio del proyecto y levantar todos los servicios:

```bash
cd docker
docker-compose up -d
```

**Servicios que se iniciarán:**
- PostgreSQL (puerto 5432)
- Zookeeper (puerto 2181)
- Kafka Broker (puertos 9092, 29092)
- Schema Registry (puerto 8081)
- Kafka Connect + Debezium (puerto 8083)
- Kafka UI (puerto 8080)
- ClickHouse (puertos 9000, 8123)

**Verificar estado:**
```bash
docker-compose ps
```

Todos los servicios deben estar en estado `healthy` o `running`.

### 2. Esperar a que los Servicios estén Listos

Los servicios tardan entre 1-3 minutos en inicializarse completamente.

**Monitorear logs:**
```bash
# Todos los servicios
docker-compose logs -f

# Servicio específico
docker-compose logs -f kafka-connect
```

### 3. Ejecutar Script de Setup

Este script configura el Debezium connector y valida el sistema:

```bash
cd ../scripts
chmod +x setup.sh
./setup.sh
```

**El script realiza:**
- ✅ Verificación de health de servicios
- ✅ Creación del Debezium connector
- ✅ Validación de topics de Kafka
- ✅ Verificación de tablas en PostgreSQL y ClickHouse

### 4. Configurar Entorno Python

Crear y activar un entorno virtual:

```bash
cd ../python

# Windows
python -m venv venv
venv\Scripts\activate

# Linux/Mac
python3 -m venv venv
source venv/bin/activate
```

Instalar dependencias:

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 5. Iniciar el Consumer Python

```bash
python consumer.py
```

**Output esperado:**
```json
{"event": "consumer_initialized", "topics": [...], "timestamp": "..."}
{"event": "consumer_started", "timestamp": "..."}
```

Mantén este proceso corriendo en una terminal.

### 6. Generar Datos de Prueba

En **otra terminal**, activar el entorno virtual y ejecutar:

```bash
cd scripts
python test_data_generator.py
```

Este script:
- Inserta 20 clientes
- Inserta 50 vehículos
- Simula operaciones continuas (INSERT, UPDATE) por 5 minutos

## 🔍 Verificación del Sistema

### Health Check Completo

```bash
cd scripts
python health_check.py
```

**Resultado esperado:**
```
✅ PostgreSQL (Source)
✅ Kafka Broker
✅ Kafka Connect
✅ Debezium Connector
✅ ClickHouse (Target)

All systems operational!
```

### Verificar Kafka Topics

**Via Kafka UI (Recomendado):**
1. Abrir navegador: http://localhost:8080
2. Ver topics con prefijo `cdc.ecommerce.*`
3. Inspeccionar mensajes

**Via CLI:**
```bash
docker exec kafka-broker kafka-topics --list --bootstrap-server localhost:9092
```

### Verificar Datos en PostgreSQL

```bash
docker exec -it postgres-source psql -U postgres -d ecommerce_db

# SQL queries
SELECT count(*) FROM customers;
SELECT count(*) FROM vehicles;
SELECT count(*) FROM orders;
```

### Verificar Datos en ClickHouse

```bash
docker exec -it clickhouse-analytics clickhouse-client

# ClickHouse queries
USE ecommerce_analytics;
SELECT count() FROM customers;
SELECT count() FROM vehicles;

# Ver datos CDC
SELECT 
    customer_id, 
    email, 
    cdc_operation, 
    cdc_timestamp 
FROM customers 
LIMIT 10;
```

## 📊 Monitoreo en Tiempo Real

### Kafka UI Dashboard

Acceder a http://localhost:8080

**Funcionalidades:**
- Ver todos los topics y su contenido
- Monitorear consumer groups y lag
- Inspeccionar schemas
- Ver configuración de connectors

### Logs del Consumer

El consumer Python genera logs estructurados en formato JSON:

```bash
# Ver logs en tiempo real
cd python
python consumer.py | jq .
```

**Métricas importantes:**
- `messages_processed`: Total de mensajes procesados
- `batches_committed`: Batches confirmados
- `messages_failed`: Mensajes con errores

### Métricas de ClickHouse

```sql
-- Rows por tabla
SELECT 
    table,
    sum(rows) as total_rows,
    formatReadableSize(sum(bytes)) as size
FROM system.parts
WHERE database = 'ecommerce_analytics'
GROUP BY table;

-- Inserts recientes
SELECT 
    event_time,
    Tables,
    ProfileEvents['InsertedRows'] as rows_inserted
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query_kind = 'Insert'
ORDER BY event_time DESC
LIMIT 10;
```

## 🛠️ Configuración Avanzada

### Ajustar Batch Size del Consumer

Editar `python/consumer.py`:

```python
config = {
    'batch_size': 100,        # Aumentar para mayor throughput
    'batch_timeout_ms': 5000  # Reducir para menor latencia
}
```

### Modificar Retención de Kafka

```bash
docker exec kafka-broker kafka-configs \
  --bootstrap-server localhost:9092 \
  --alter \
  --entity-type topics \
  --entity-name cdc.ecommerce.ecommerce_postgres.public.customers \
  --add-config retention.ms=604800000
```

### Optimizar ClickHouse

```sql
-- Ejecutar OPTIMIZE para mergear parts
OPTIMIZE TABLE ecommerce_analytics.customers FINAL;

-- Configurar compression
ALTER TABLE ecommerce_analytics.vehicles 
MODIFY SETTING compress_marks = 0, compress_primary_key = 1;
```

## 🧪 Testing

### Test de Latencia End-to-End

1. Insertar un registro en PostgreSQL
2. Verificar cuánto tarda en aparecer en ClickHouse

```bash
# Terminal 1: Monitorear ClickHouse
watch -n 1 'docker exec clickhouse-analytics clickhouse-client -q "SELECT count() FROM ecommerce_analytics.customers"'

# Terminal 2: Insertar en PostgreSQL
docker exec postgres-source psql -U postgres -d ecommerce_db -c "
  INSERT INTO customers (first_name, last_name, email) 
  VALUES ('Test', 'User', 'test.user@example.com')
"
```

**Latencia esperada:** < 5 segundos

### Test de Throughput

```bash
cd scripts
python test_data_generator.py
```

Monitorear métricas del consumer para verificar throughput.

## 🐛 Troubleshooting

### Issue: Connector No Se Crea

**Síntomas:** Error al ejecutar `setup.sh`

**Solución:**
```bash
# Verificar logs de Kafka Connect
docker logs kafka-connect-debezium

# Reintentar manualmente
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @../debezium/connectors/postgres-connector.json
```

### Issue: Consumer No Recibe Mensajes

**Verificar:**
1. Connector está en estado RUNNING
2. Topics existen en Kafka
3. Consumer está suscrito a topics correctos

```bash
# Estado del connector
curl http://localhost:8083/connectors/postgres-ecommerce-connector/status | jq .

# Listar topics
docker exec kafka-broker kafka-topics --list --bootstrap-server localhost:9092

# Ver mensajes en topic
docker exec kafka-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cdc.ecommerce.ecommerce_postgres.public.customers \
  --from-beginning \
  --max-messages 5
```

### Issue: ClickHouse Connection Failed

**Verificar puerto y credenciales:**
```bash
docker exec clickhouse-analytics clickhouse-client --query "SELECT 1"

# Verificar si acepta conexiones externas
netstat -an | grep 9000
```

### Issue: Alta Latencia

**Causas comunes:**
- Batch size muy grande
- Recursos insuficientes
- Network issues

**Optimizaciones:**
```python
# Reducir batch size
config['batch_size'] = 50

# Reducir timeout
config['batch_timeout_ms'] = 2000
```

## 🔄 Actualización y Mantenimiento

### Actualizar Imagen de Docker

```bash
cd docker
docker-compose pull
docker-compose up -d
```

### Backup de Datos

**PostgreSQL:**
```bash
docker exec postgres-source pg_dump -U postgres ecommerce_db > backup.sql
```

**ClickHouse:**
```bash
docker exec clickhouse-analytics clickhouse-client --query "
  BACKUP TABLE ecommerce_analytics.customers 
  TO Disk('backups', 'customers_backup.zip')
"
```

### Limpiar Datos de Prueba

```bash
# PostgreSQL
docker exec postgres-source psql -U postgres -d ecommerce_db -c "
  TRUNCATE customers, vehicles, orders, order_items, payments CASCADE;
"

# ClickHouse
docker exec clickhouse-analytics clickhouse-client --query "
  TRUNCATE TABLE ecommerce_analytics.customers;
  TRUNCATE TABLE ecommerce_analytics.vehicles;
  TRUNCATE TABLE ecommerce_analytics.orders;
  TRUNCATE TABLE ecommerce_analytics.order_items;
  TRUNCATE TABLE ecommerce_analytics.payments;
"
```

## 📚 Recursos Adicionales

- [Debezium Documentation](https://debezium.io/documentation/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [ClickHouse Documentation](https://clickhouse.com/docs/)
- [Confluent Kafka Python](https://docs.confluent.io/kafka-clients/python/current/overview.html)

## 🆘 Soporte

Si encuentras problemas:

1. Revisa los logs: `docker-compose logs -f [service]`
2. Ejecuta health check: `python scripts/health_check.py`
3. Revisa la documentación en `/docs`
4. Verifica recursos del sistema: `docker stats`
