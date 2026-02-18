# ⚡ API de Tarifas de Energía – Mercado Regulado

API desarrollada con **FastAPI** y **PostgreSQL** para la gestión, procesamiento y análisis de tarifas de energía eléctrica del **mercado regulado en Colombia**.

El servicio implementa un proceso ETL completo para consumir datos oficiales, transformarlos, almacenarlos en base de datos y exponer estadísticas para su análisis.

---

## 📌 Descripción

Esta API gestiona información relacionada con:

- Tarifas de energía
- Costo de compra de energía
- Cargos de transporte (Transmisión y Distribución)
- Márgenes de comercialización
- Costos asociados a generación y pérdidas
- Factores operativos que influyen en el precio final

Los datos:

- Se desglosan por tipo de propiedad:
  - Operador de Red
  - Propiedad Compartida
  - Propiedad Cliente
- Se dividen por niveles de tensión eléctrica
- Cubren el período **enero 2024 – septiembre 2025**
- Permiten análisis por región y período

Fuente de datos: API pública de **datos.gov.co**

---

# 🏗️ Arquitectura del Proyecto

app/
│
├── config/
│ ├── env.py
│ └── logger.py
│
├── database/
│ └── postgresql.py
│
├── etl/
│ ├── extract.py
│ ├── transform.py
│ ├── loader.py
│ └── pipeline.py
│
├── routes/
│ ├── etl.py
│ └── estadisticas.py
│
└── main.py


---

# ⚙️ Tecnologías Utilizadas

- Python
- FastAPI
- PostgreSQL
- psycopg3
- Pandas
- Uvicorn

---

# 🗄️ Base de Datos

El módulo `database/postgresql.py` contiene la clase `Database`, que:

- Inicializa la conexión a PostgreSQL
- Funciones para consulta, transacciones, ejecutar procedimientos o funciones

---

# 🔄 Proceso ETL

El sistema implementa un flujo ETL estructurado en cuatro componentes principales:

## 1️⃣ Extract (`extract.py`)

- Consume datos desde la API pública de datos.gov.co
- Extrae la información en formato estructurado
- Maneja errores de conexión

---

## 2️⃣ Transform (`transform.py`)

Durante la transformación se realiza:

- Limpieza de espacios adicionales
- Eliminación de texto innecesario (por ejemplo, contenido entre paréntesis)
- Conversión de tipos de datos (string → numérico cuando corresponde)
- Normalización de texto a `UPPERCASE`
- Separación de región y operador
- Validación de:
  - Valores nulos
  - Campos vacíos
  - Registros duplicados
- Estandarización de estructura

---

## 3️⃣ Load (`loader.py`)

- Cargue masivo (bulk insert) a PostgreSQL
- Optimización para grandes volúmenes de datos
- Control de integridad

---

## 4️⃣ Pipeline (`pipeline.py`)

Orquesta el proceso completo:

```text
Inicio
   ↓
Extract
   ↓
Transform
   ↓
Load
   ↓
Resultado final

Responsabilidades del Pipeline:

- Ejecutar cada etapa en orden
- Manejar excepciones
- Registrar logs
- Retornar resultado estructurado
- Permitir notificación de éxito o fallo