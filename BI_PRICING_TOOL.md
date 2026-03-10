# Swift Pricing Intelligence — Proyecto BI de Formación de Precios y P&L

> Extensión natural de **Swift Sales Intelligence** hacia la gestión comercial completa:
> de la venta al margen, de la verba al resultado.

---

## 1. Contexto y Problema

Hoy el proceso de precios funciona así:

```
Lista de precios (Excel) 
    → Script Python manual 
        → Verbas por canal (Excel) 
            → HTA por cliente (Excel) 
                → BI abre archivos uno a uno
```

**Consecuencias:**
- El conocimiento vive en los archivos, no en el sistema
- Cada mes son horas de trabajo manual susceptible a errores
- P&L real (facturación vs. condición acordada) no existe
- Sin alertas, sin historial auditable, sin drill-down dinámico

---

## 2. Objetivo del Proyecto

Construir una herramienta **local-first, offline, integrada con el sales_app existente** que permita:

1. **Formar precios** por canal con lógica de negocio codificada (no en cabezas)
2. **Ver P&L real** comparando la verba acordada vs. la facturación efectiva
3. **Auditar cambios** con historial de quién modificó qué y cuándo
4. **Alertar automáticamente** cuando un margen cae debajo del umbral

---

## 3. Arquitectura Propuesta

```
┌─────────────────────────────────────────────────────────┐
│                     FUENTES DE DATOS                    │
│  Excel Listas de Precios  │  TXT/Excel Facturación      │
│  (PricesList/2026-Mar/)   │  (data/Facturación*.txt)    │
└──────────────┬────────────┴──────────┬──────────────────┘
               │  ETL (etl.py ampliado)│
               ▼                       ▼
┌─────────────────────────────────────────────────────────┐
│                    SQLite (db/app.db)                    │
│                                                         │
│  prices_list   verbas    canales    facturacion         │
│  ──────────    ──────    ───────    ───────────         │
│  sku           sku       id         sku                 │
│  canal         canal     nombre     canal               │
│  precio        precio_e  tipo       precio_real         │
│  periodo       dcto_f               periodo             │
│  vigencia      precio_g             cliente_id          │
│                periodo                                  │
│                usuario                                  │
│                created_at                               │
└──────────────────────────┬──────────────────────────────┘
                           │  Flask API (app.py ampliado)
                           ▼
┌─────────────────────────────────────────────────────────┐
│                    MÓDULOS FRONTEND                     │
│                                                         │
│  [Dashboard]   [Formador de Precios]   [P&L por Canal] │
│  [Verbas]      [Análisis Competencia]  [Alertas]       │
└─────────────────────────────────────────────────────────┘
```

---

## 4. Módulos del Sistema

### 4.1 Módulo: Formador de Precios

**Qué hace:** reemplaza el script Python manual de hoy.

- Carga automática de listas de precios desde Excel al DB
- Reglas de negocio configurables por canal:
  - DH: `E = Px SubDx`, `F = 1-(Px Dx / Px SubDx)` para untables
  - MAY: `E = Px Mayorista`, `F = descuento histórico fijo`
  - MB: `PPA proporcional al aumento de lista`
  - SUP: `E = Px Super Regional`, `F = descuento fijo`
  - RV: `E = Px Super`, `G = Px Rosario Compras`, `F = 1-(G/E)`
- Exporta verbas Excel con un clic (usando lógica actual de scripts)
- **Historial**: cada generación queda registrada con fecha y usuario

### 4.2 Módulo: P&L por Canal

**Qué hace:** cruza la verba (condición acordada) con la facturación real.

```
P&L = Facturación real por SKU/canal 
    vs. Precio_G de la verba vigente en ese período
```

Métricas por pantalla:
| Métrica | Fórmula |
|---|---|
| Contribución marginal real | `Precio facturado - Costo variable` |
| GAP verba vs. factura | `(Precio_G - Precio facturado) / Precio_G` |
| Volumen en riesgo | SKUs donde GAP > umbral configurable |
| Evolución mes a mes | Comparativa E, F, G por período |

### 4.3 Módulo: Análisis de Competencia

**Qué hace:** extiende las planillas de análisis actuales (MB, SUP) al sistema.

- Carga manual o automática de precios de competencia por categoría
- Calcula GAP: `(Comp - PPA) / Comp`
- Alerta si GAP cae por debajo de umbral (ej: < 5%)
- Gráfico de tendencia: nuestro PPA vs. competencia mes a mes

### 4.4 Módulo: Alertas Automáticas

| Alerta | Condición |
|---|---|
| Margen crítico | B.Even < X% en algún SKU/canal |
| GAP cerrado | Competencia se acerca a nuestro PPA |
| Verba vencida | Período de vigencia expirado |
| Precio desactualizado | Lista de precios > 30 días sin actualizar |

### 4.5 Módulo: Auditoría

- Log de cada cambio: `usuario | timestamp | canal | sku | campo | valor_anterior | valor_nuevo`
- Permite reconstruir cualquier verba histórica
- Exportable a Excel para revisión externa

---

## 5. Base de Datos — Esquema Principal

```sql
-- Canales comerciales
CREATE TABLE canales (
    id TEXT PRIMARY KEY,          -- 'DH', 'MAY', 'MB', 'SUP', 'RV'
    nombre TEXT,
    tipo TEXT                     -- 'distribuidor', 'mayorista', 'supermercado'
);

-- Listas de precios cargadas
CREATE TABLE prices_list (
    id INTEGER PRIMARY KEY,
    sku INTEGER,
    descripcion TEXT,
    canal TEXT REFERENCES canales(id),
    precio REAL,
    periodo TEXT,                 -- '2026-03'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Verbas generadas
CREATE TABLE verbas (
    id INTEGER PRIMARY KEY,
    sku INTEGER,
    canal TEXT REFERENCES canales(id),
    precio_e REAL,               -- Precio de lista
    dcto_f REAL,                 -- Descuento
    precio_g REAL,               -- Precio factura
    ppa REAL,                    -- Precio Público Apuntado
    periodo_desde DATE,
    periodo_hasta DATE,
    usuario TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Auditoría de cambios
CREATE TABLE auditoria (
    id INTEGER PRIMARY KEY,
    tabla TEXT,
    registro_id INTEGER,
    campo TEXT,
    valor_anterior TEXT,
    valor_nuevo TEXT,
    usuario TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## 6. Stack Tecnológico

| Capa | Tecnología | Por qué |
|---|---|---|
| Backend | Python + Flask | Ya existe, equipo lo conoce |
| Base de datos | SQLite → PostgreSQL | Empezar local, escalar si hace falta |
| ETL | etl.py ampliado | Reutilizar lógica existente |
| Frontend | HTML + JS (Noir Intelligence) | Ya existe el design system |
| Gráficos | Chart.js | Liviano, sin dependencias externas |
| Export Excel | openpyxl | Ya se usa en el proyecto |
| Auth (futuro) | Flask-Login | Simple, local-first |

---

## 7. Roadmap

### Fase 1 — Base de datos de precios (2 semanas)
- [ ] Diseñar y crear tablas `prices_list`, `verbas`, `canales`
- [ ] ETL: importar listas Excel → DB automáticamente
- [ ] ETL: importar verbas históricas (ENE, FEB, MAR 2026)
- [ ] API: endpoints `/api/prices`, `/api/verbas`

### Fase 2 — Formador de precios (2 semanas)
- [ ] UI para cargar nueva lista de precios del mes
- [ ] Motor de reglas por canal (DH, MAY, MB, SUP, RV)
- [ ] Generación automática de verbas Excel desde DB
- [ ] Historial y auditoría de cambios

### Fase 3 — P&L real (3 semanas)
- [ ] Cruzar facturación (ya en DB desde `etl.py`) con verbas
- [ ] Dashboard P&L por canal, por SKU, por período
- [ ] Comparativa verba vs. facturación real
- [ ] Exportar P&L a Excel

### Fase 4 — Alertas y análisis competencia (2 semanas)
- [ ] Módulo de carga de precios de competencia
- [ ] Cálculo automático de GAP
- [ ] Sistema de alertas por margen y GAP
- [ ] Notificaciones en dashboard

---

## 8. Lo que se Reutiliza del Proyecto Actual

| Componente actual | Rol en el nuevo sistema |
|---|---|
| `etl.py` | Ampliado para procesar también listas de precios y verbas |
| `app.py` | Nuevos endpoints para pricing y P&L |
| `db/app.db` | Se agregan las nuevas tablas al mismo archivo |
| Diseño Noir Intelligence | Mismo design system para los nuevos módulos |
| Scripts Python de verbas | Se encapsulan como funciones del backend |
| Verbas ENE/FEB/MAR 2026 | Se importan como historial inicial |

---

## 9. Qué Cambia Para el Usuario

**Hoy:**
1. Recibir lista Excel → correr script Python → revisar verbas → mandar por mail

**Con este sistema:**
1. Subir lista Excel al sistema → el motor genera las verbas → revisar en dashboard → exportar y mandar

El proceso de horas se convierte en minutos. Y el P&L deja de ser una promesa y pasa a ser una pantalla.

---

## 10. Riesgos y Mitigación

| Riesgo | Mitigación |
|---|---|
| Cambios en formato de listas Excel del proveedor | ETL con detección automática de columnas + alertas |
| Reglas de negocio cambian mes a mes | Motor de reglas configurable desde UI, no hardcodeado |
| Pérdida de datos | Backup automático de SQLite antes de cada ETL |
| Adopción lenta del equipo | Mantener export Excel: el sistema genera los mismos archivos de siempre |

---

*Proyecto: Swift Pricing Intelligence — extensión de Swift Sales Intelligence*
*Autor: Nicolas Gentile — Sales Ops*
*Fecha: Marzo 2026*
