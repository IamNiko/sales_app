import sqlite3
import re

db_path = '/home/niko/cyber/sales_app/db/app.db'
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

# Get distinct plazos
cursor.execute("SELECT DISTINCT plazo FROM dim_clients")
rows = cursor.fetchall()
print("Valores distintos de 'plazo' en dim_clients:")
for row in rows:
    print(repr(row['plazo']))

# Check if there are some overdue invoices based on a naïve logic
print("\nEjemplo de facturas y plazos:")
cursor.execute("""
    SELECT f.fecha_emision, f.importe, c.plazo
    FROM fact_facturacion f
    JOIN dim_clients c ON f.cod_cliente = c.cliente_id
    WHERE c.plazo IS NOT NULL AND c.plazo != ''
    LIMIT 20
""")
for r in cursor.fetchall():
    print(r['fecha_emision'], r['importe'], r['plazo'])

conn.close()
