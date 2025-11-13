# CDC Pipeline - Ecommerce de Autos

## 🚀 Arquitectura del Proyecto

Pipeline de **Change Data Capture (CDC)** en tiempo real para un ecommerce de ventas de autos, utilizando tecnologías open source de última generación.

### Stack Tecnológico

- **PostgreSQL**: Base de datos transaccional (OLTP)
- **Debezium**: Captura de cambios en tiempo real (CDC)
- **Apache Kafka**: Streaming de eventos distribuido
- **ClickHouse**: Base de datos analítica columnar (OLAP)
- **Python**: Procesamiento y transformación de datos
- **Docker Compose**: Orquestación de servicios

## 📊 Arquitectura

```
PostgreSQL (OLTP)
    ↓ (Debezium CDC Connector)
Apache Kafka (Event Stream)
    ↓ (Python Consumer)
Transformaciones en Tiempo Real
    ↓
ClickHouse (OLAP Analytics)
```

## 🗂️ Estructura del Proyecto

```
20251113_cdc/
├── docker/
│   ├── docker-compose.yml
│   └── .env
├── postgres/
│   ├── init/
│   │   └── 01-schema.sql
│   └── config/
│       └── postgresql.conf
├── debezium/
│   └── connectors/
│       └── postgres-connector.json
├── clickhouse/
│   └── init/
│       └── 01-schema.sql
├── kafka/
│   └── config/
│       └── server.properties
├── python/
│   ├── requirements.txt
│   ├── consumer.py
│   ├── transformers.py
│   └── clickhouse_loader.py
├── scripts/
│   ├── test_data_generator.py
│   ├── setup.sh
│   └── health_check.py
├── docs/
│   ├── architecture.md
│   └── setup_guide.md
└── README.md
```

## 🎯 Características

- ✅ Captura de cambios en tiempo real (INSERT, UPDATE, DELETE)
- ✅ Procesamiento stream con baja latencia
- ✅ Transformaciones en memoria
- ✅ Storage optimizado para analytics
- ✅ Escalabilidad horizontal
- ✅ Recuperación ante fallos
- ✅ Monitoreo y logging completo

## 🚦 Quick Start

```bash
# 1. Iniciar servicios
cd docker
docker-compose up -d

# 2. Configurar Debezium connector
./scripts/setup.sh

# 3. Iniciar consumer Python
cd python
pip install -r requirements.txt
python consumer.py

# 4. Generar datos de prueba
python scripts/test_data_generator.py
```

## 📋 Modelo de Datos

### Tablas Transaccionales (PostgreSQL)
- `customers` - Clientes del ecommerce
- `vehicles` - Inventario de vehículos
- `orders` - Órdenes de compra
- `order_items` - Detalle de items
- `payments` - Transacciones de pago

### Tablas Analíticas (ClickHouse)
- Misma estructura optimizada para queries OLAP
- Índices columnares
- Particionamiento por fecha

## 🔧 Configuración

Ver documentación detallada en `/docs/setup_guide.md`

## 📈 Monitoreo

- Kafka UI: http://localhost:8080
- ClickHouse Client: `docker exec -it clickhouse clickhouse-client`
- PostgreSQL: `psql -h localhost -U postgres -d ecommerce_db`

## 🎓 Caso de Uso

Sistema para ecommerce de venta de autos que requiere:
- Dashboard en tiempo real de ventas
- Analytics de inventario actualizado
- Tracking de transacciones
- Reports históricos con datos actualizados

---

**Desarrollado con las mejores prácticas de Data Engineering**
