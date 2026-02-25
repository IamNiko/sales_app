#!/usr/bin/env python3
"""
JSON Exporter for Sales Dashboard
Exports SQLite data to JSON files for offline dashboard consumption.

Usage:
  python export_json.py --db-path db/app.db --output-dir data
"""

import sqlite3
import json
import argparse
import logging
from pathlib import Path
from datetime import datetime


class JSONExporter:
    def __init__(self, db_path, output_dir):
        self.db_path = Path(db_path)
        self.output_dir = Path(output_dir)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row  # Enable dict-like access
        self.metadata = {
            "generated_at": datetime.now().isoformat(),
            "database": str(self.db_path)
        }

    def export_sales(self):
        """Export consolidated sales with full enrichment."""
        logging.info("Exporting sales data...")
        
        query = """
            SELECT 
                f.fecha_emision as date,
                f.cod_vendedor as seller_code,
                f.cod_cliente as customer_code,
                f.cod_producto as product_code,
                f.cantidad as units_kg,
                f.importe as amount,
                f.deposito,
                f.year_month,
                f.es_premium,
                p.descripcion as product_name,
                p.categoria as product_category,
                p.subcategoria as product_subcategory,
                c.cliente_name as customer_name,
                c.frecuencia as customer_frequency
            FROM fact_facturacion f
            LEFT JOIN dim_product_classification p ON f.cod_producto = p.cod_producto
            LEFT JOIN dim_clients c ON f.cod_cliente = c.cliente_id
            ORDER BY f.fecha_emision
        """
        
        cursor = self.conn.execute(query)
        sales = [dict(row) for row in cursor.fetchall()]
        
        # Calculate metadata
        if sales:
            dates = [s['date'] for s in sales if s['date']]
            self.metadata['sales'] = {
                "total_rows": len(sales),
                "date_range": {
                    "min": min(dates) if dates else None,
                    "max": max(dates) if dates else None
                }
            }
        
        output = {
            "sales": sales,
            "metadata": self.metadata
        }
        
        output_path = self.output_dir / "sales_consolidated.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        
        logging.info(f"✓ Exported {len(sales)} sales records to {output_path}")
        return len(sales)

    def export_targets(self):
        """Export seller objectives."""
        logging.info("Exporting targets...")
        
        query = """
            SELECT 
                cod_vendedor as seller_code,
                nom_vendedor as seller_name,
                year_month as period,
                objetivo_pesos as target_amount,
                objetivo_premium_pesos as target_premium,
                objetivo_kg as target_kg
            FROM vendedor_objetivos
            ORDER BY year_month DESC, nom_vendedor
        """
        
        cursor = self.conn.execute(query)
        targets = [dict(row) for row in cursor.fetchall()]
        
        output = {
            "targets": targets,
            "metadata": {
                "generated_at": self.metadata["generated_at"],
                "total_sellers": len(targets)
            }
        }
        
        output_path = self.output_dir / "targets.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        
        logging.info(f"✓ Exported {len(targets)} target records to {output_path}")
        return len(targets)

    def export_clients(self):
        """Export client dimension."""
        logging.info("Exporting clients master...")
        
        query = """
            SELECT 
                cliente_id as code,
                cliente_name as name,
                cod_centralizador as centralizador_code,
                frecuencia as frequency
            FROM dim_clients
            ORDER BY cliente_name
        """
        
        cursor = self.conn.execute(query)
        clients = [dict(row) for row in cursor.fetchall()]
        
        output = {
            "clients": clients,
            "metadata": {
                "generated_at": self.metadata["generated_at"],
                "total_clients": len(clients)
            }
        }
        
        output_path = self.output_dir / "clients_master.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        
        logging.info(f"✓ Exported {len(clients)} client records to {output_path}")
        return len(clients)

    def export_products(self):
        """Export product classification."""
        logging.info("Exporting product classification...")
        
        query = """
            SELECT 
                cod_producto as code,
                descripcion as name,
                categoria as category,
                subcategoria as subcategory,
                CASE WHEN subcategoria = 'PREMIUM' THEN 1 ELSE 0 END as is_premium
            FROM dim_product_classification
            ORDER BY categoria, descripcion
        """
        
        cursor = self.conn.execute(query)
        products = [dict(row) for row in cursor.fetchall()]
        
        output = {
            "products": products,
            "metadata": {
                "generated_at": self.metadata["generated_at"],
                "total_products": len(products)
            }
        }
        
        output_path = self.output_dir / "products_classification.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        
        logging.info(f"✓ Exported {len(products)} product records to {output_path}")
        return len(products)

    def export_historical_sales(self):
        """Export historical monthly sales by client (from Excel)."""
        logging.info("Exporting historical sales (Excel source)...")
        
        query = """
            SELECT 
                cod_cliente as client_code,
                cod_vendedor as seller_code,
                year_month,
                kg_vendidos as kg_sold
            FROM fact_cliente_historico
            ORDER BY year_month, cod_vendedor, cod_cliente
        """
        
        cursor = self.conn.execute(query)
        historical = [dict(row) for row in cursor.fetchall()]
        
        output = {
            "historical_sales": historical,
            "metadata": {
                "generated_at": self.metadata["generated_at"],
                "source": "Avance x Cliente-Vendedor (Excel)",
                "total_records": len(historical)
            }
        }
        
        output_path = self.output_dir / "historical_sales.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        
        logging.info(f"✓ Exported {len(historical)} historical records to {output_path}")
        return len(historical)

    def export_avance_vendedor(self):
        """Export current month's Avance Cliente-Vendedor data."""
        logging.info("Exporting Avance Cliente-Vendedor...")
        
        query = """
            SELECT 
                year_month,
                canal,
                zona,
                jefe,
                cod_vendedor as seller_code,
                nom_vendedor as seller_name,
                cod_cliente as client_code,
                nom_cliente as client_name,
                venta_actual as current_sales,
                objetivo as target,
                pendiente as pending,
                frecuencia as frequency,
                match_quality
            FROM fact_avance_cliente_vendedor_month
            ORDER BY year_month DESC, nom_vendedor, nom_cliente
        """
        
        cursor = self.conn.execute(query)
        avance = [dict(row) for row in cursor.fetchall()]
        
        output = {
            "avance_vendedor": avance,
            "metadata": {
                "generated_at": self.metadata["generated_at"],
                "total_records": len(avance)
            }
        }
        
        output_path = self.output_dir / "avance_vendedor.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        
        logging.info(f"✓ Exported {len(avance)} avance records to {output_path}")
        return len(avance)

    def export_all(self):
        """Export all datasets to JSON."""
        logging.info("=== Starting JSON Export ===")
        
        totals = {
            "sales": self.export_sales(),
            "targets": self.export_targets(),
            "clients": self.export_clients(),
            "products": self.export_products(),
            "historical": self.export_historical_sales(),
            "avance_vendedor": self.export_avance_vendedor()
        }
        
        logging.info("=== JSON Export Complete ===")
        logging.info(f"Summary: {totals}")
        
        # Export summary
        summary_path = self.output_dir / "export_summary.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump({
                "exported_at": self.metadata["generated_at"],
                "totals": totals
            }, f, indent=2)
        
        return totals


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export Sales DB to JSON")
    parser.add_argument("--db-path", default="db/app.db", help="SQLite database path")
    parser.add_argument("--output-dir", default="data", help="Output directory for JSON files")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    
    args = parser.parse_args()
    
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s [%(levelname)s] %(message)s'
    )
    
    exporter = JSONExporter(args.db_path, args.output_dir)
    exporter.export_all()
