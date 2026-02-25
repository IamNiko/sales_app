# Swift Sales Intelligence 📊

Panel de control de ventas profesional, offline y local, diseñado para la gestión comercial avanzada de distribuidores.

## 🚀 Características Principales

- **Dashboard Unificado**: Visualización de objetivos vs. facturación real en tiempo real.
- **Tracking de Coberturas**: Análisis detallado de la adopción de productos de lanzamiento (Papas, Veggies, Untables, etc.).
- **Motor ETL**: Procesamiento automático de archivos XLSX y TXT (formatos Legacy y Minerva).
- **Ficha de Cliente**: Historial de compras, evolución mensual y semáforo de coberturas.
- **Modo Offline**: Arquitectura local-first para máxima velocidad y privacidad.

## 🛠 Estructura del Proyecto

- `app.py`: Servidor Flask y API de datos.
- `etl.py`: Procesador de datos (limpieza, normalización y carga a SQLite).
- `templates/`: Interfaces HTML modernas bajo el diseño **Noir Intelligence**.
- `db/app.db`: Base de datos SQLite relacional.
- `data/`: Directorio de archivos fuente (Excel de objetivos, lanzamientos y facturación).

## 💻 Instalación y Uso

### 1. Requisitos
- Python 3.10+
- Flask e itables

### 2. Procesar Datos (ETL)
Para cargar los archivos Excel del directorio `data/` a la base de datos:
```bash
python etl.py
```

### 3. Iniciar el Servidor
```bash
python app.py
```
Luego navegar a `http://localhost:5000`.

## 📈 Métricas de Cobertura
El sistema utiliza el archivo `Compradores Lanzamientos.xlsx` para trackear el estado de 7 productos clave, permitiendo filtrar por Vendedor, Jefe o Zona para identificar oportunidades de mercado no explotadas.

---
*Desarrollado para Nicolas Gentile - Sales Ops Intelligence*
