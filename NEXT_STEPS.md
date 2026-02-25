# Next Steps - Sales ETL Upgrade

## ✅ Completado

1. **Multi-TXT Parser** - `etl.py` ahora procesa múltiples archivos `Facturación*.txt`
2. **JSON Exporter** - `export_json.py` exporta 6 datasets a JSON
3. **CLI Integration** - Flag `--export-json` agregado

## 📋 Pendiente - Esperando archivos

Necesitamos agregar a `/data`:
- `Facturación Nov 2025.txt`
- `Facturación Dic 2025.txt`
- `Facturación Ene 2026.txt`

## 🚀 Una vez que tengas los archivos

```bash
# Backup DB actual
cp db/app.db db/app.db.backup

# Ejecutar ETL completo con export
python etl.py --data-dir data --export-json

# Verificar JSONs generados
ls -lh data/*.json
```

## 📊 Archivos JSON que se generarán

- `data/sales_consolidated.json` - Ventas completas (nov 2025 → hoy)
- `data/historical_sales.json` - Histórico Excel (ene-oct 2025)
- `data/targets.json` - Objetivos por vendedor
- `data/clients_master.json` - Maestro de clientes
- `data/products_classification.json` - Clasificación productos
- `data/avance_vendedor.json` - Avance mensual actual
- `data/export_summary.json` - Resumen de la exportación

---

**Estado:** Esperando archivos del usuario para testing completo.
