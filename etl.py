#!/usr/bin/env python3
"""
Sales Ops ETL Engine - v1.0
Author: Archi (Antigravity)

Usage:
  python etl.py --data-dir data --db-path db/app.db --log-path logs/etl.log

Required File Naming Patterns in --data-dir:
- Facturación.txt                          (Semicolon delimited)
- Clientes_master.xlsx                     (Dimension)
- CLASIFICACION DE PRODUCTOS AR.xlsx       (Dimension)
- Avance x Cliente-Vendedor DD-MM.xlsx     (Current month updater)
- Avance General DD-MM.xlsx                (Current month updater)
- Stock vs Pendiente DD-MM.xlsx            (Current month updater)
"""

import os
import sys
import argparse
import logging
import sqlite3
import re
import json
import hashlib
from datetime import datetime
from pathlib import Path
import pandas as pd
import numpy as np

# --- CONFIGURATION & GLOBALS ---
REQUIRED_FILES = {
    'facturacion': 'Facturación.txt',
    'clients': 'Clientes_master.xlsx',
    'products': 'CLASIFICACION DE PRODUCTOS AR.xlsx',
    'avance_vendedor': r'Avance x Cliente-Vendedor.*\.xlsx',
    'avance_general': r'Avance General.*\.xlsx',
    'stock_pendiente': r'Stock vs Pendiente.*\.xlsx'
}

# --- HELPERS ---

def setup_logging(log_path):
    log_dir = os.path.dirname(log_path)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler(sys.stdout)
        ]
    )

def normalize_text(text):
    if pd.isna(text): return ""
    text = str(text).strip().upper()
    text = re.sub(r'\s+', ' ', text)
    # Remove accents/special chars for name matching
    import unicodedata
    text = "".join(c for c in unicodedata.normalize('NFKD', text) if not unicodedata.combining(c))
    return text

def normalize_key(text):
    """Normalize vendor/client IDs: strip whitespace, remove trailing .0 from numeric floats."""
    if pd.isna(text): return ""
    s = str(text).strip()
    # Remove trailing .0 from numeric IDs common in Excel/TXT imports (e.g. 100067806.0 -> 100067806)
    if s.endswith('.0'):
        s = s[:-2]
    return s.zfill(5) if s.isdigit() else s.upper()


def coerce_numeric(val):
    if isinstance(val, str):
        # Handle "46.774,19" -> 46774.19
        val = val.replace('.', '').replace(',', '.')
    try:
        return float(val)
    except:
        return 0.0

def parse_date_from_filename(filename, year_override=None):
    """Detects DD, MM from filename pattern 'Something 06-02.xlsx'.
    Falls back to current month if no date pattern found."""
    match = re.search(r'(\d{2})[-/](\d{2})', filename)
    if match:
        day, month = map(int, match.groups())
        year = year_override or datetime.now().year
        return f"{year}-{month:02d}-{day:02d}", f"{year}-{month:02d}"
    # Fallback: use current month
    now = datetime.now()
    year = year_override or now.year
    logging.warning(f"No date pattern in filename '{filename}', falling back to current month: {year}-{now.month:02d}")
    return f"{year}-{now.month:02d}-01", f"{year}-{now.month:02d}"

def clean_header(s):
    """Normalize underscores/spaces/newlines for matching."""
    s = str(s).upper().replace('_', ' ').replace('\n', ' ').strip()
    return re.sub(r'\s+', ' ', s)

def robust_read_excel(path, required_cols):
    """Searches first 20 rows for headers. Normalizes underscores/spaces for matching."""
    norm_required = [clean_header(c) for c in required_cols]
    logging.info(f"Searching for columns: {norm_required} in {path}")
    
    for i in range(20):
        try:
            df = pd.read_excel(path, header=i)
            # Normalize found columns for comparison
            found_cols = {clean_header(c): c for c in df.columns if not pd.isna(c)}
            
            if all(req in found_cols for req in norm_required):
                logging.info(f"Found headers at row {i}")
                return df
                
            if i == 0:
                logging.info(f"Row 0 headers: {list(found_cols.keys())[:10]}...")
        except Exception as e:
            continue
    
    # Final failure log
    all_found = []
    try:
        df_top = pd.read_excel(path, nrows=1)
        all_found = [clean_header(c) for c in df_top.columns]
    except: pass
    
    raise ValueError(f"Could not find {norm_required} in {path}. Found: {all_found[:15]}...")

# --- ETL CORE ---

class SalesETL:
    def __init__(self, data_dir, db_path, year_override=None):
        self.data_dir = Path(data_dir)
        self.db_path = Path(db_path)
        self.year = year_override or datetime.now().year
        self.os_created_dirs()
        self.conn = sqlite3.connect(self.db_path)
        self.run_id = None
        self.target_month = None # YYYY-MM
        self.processed_files = []

    def os_created_dirs(self):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def init_db(self):
        cursor = self.conn.cursor()
        cursor.executescript("""
            CREATE TABLE IF NOT EXISTS etl_run (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT,
                message TEXT,
                month_updated TEXT,
                files_json TEXT
            );
            CREATE TABLE IF NOT EXISTS etl_unmatched_clients (
                run_id INTEGER,
                year_month TEXT,
                cod_cliente TEXT,
                nom_cliente TEXT,
                cod_centralizador TEXT,
                reason TEXT
            );
            CREATE TABLE IF NOT EXISTS dim_clients (
                cliente_id TEXT PRIMARY KEY,
                cliente_name TEXT,
                cod_centralizador TEXT,
                frecuencia TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS dim_product_classification (
                cod_producto TEXT PRIMARY KEY,
                descripcion TEXT,
                categoria TEXT,
                subcategoria TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS fact_facturacion (
                row_hash TEXT PRIMARY KEY,
                fecha_emision TEXT,
                cod_cliente TEXT,
                cod_vendedor TEXT,
                cod_producto TEXT,
                cantidad REAL,
                importe REAL,
                deposito TEXT,
                year_month TEXT,
                es_premium INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS fact_avance_cliente_vendedor_month (
                year_month TEXT,
                canal TEXT,
                zona TEXT,
                jefe TEXT,
                cod_vendedor TEXT,
                nom_vendedor TEXT,
                cod_cliente TEXT,
                nom_cliente TEXT,
                cod_centralizador TEXT,
                venta_actual REAL,
                objetivo REAL,
                pendiente REAL,
                facturacion_pesos REAL DEFAULT 0,
                objetivo_pesos REAL DEFAULT 0,
                objetivo_premium_pesos REAL DEFAULT 0,
                frecuencia TEXT,
                match_quality TEXT
            );
            CREATE TABLE IF NOT EXISTS fact_cliente_historico (
                cod_cliente TEXT,
                cod_vendedor TEXT,
                year_month TEXT,
                kg_vendidos REAL,
                PRIMARY KEY (cod_cliente, year_month)
            );
            CREATE TABLE IF NOT EXISTS vendedor_objetivos (
                cod_vendedor TEXT PRIMARY KEY,
                nom_vendedor TEXT,
                year_month TEXT,
                objetivo_pesos REAL,
                objetivo_premium_pesos REAL,
                objetivo_kg REAL,
                objetivo_rebozados_kg REAL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS fact_lanzamiento_cobertura (
                year_month TEXT,
                lanzamiento TEXT,
                cod_vendedor TEXT,
                nom_vendedor TEXT,
                cod_cliente TEXT,
                nom_cliente TEXT,
                canal TEXT,
                zona TEXT,
                estado TEXT,
                fact_feb REAL DEFAULT 0,
                pend_feb REAL DEFAULT 0,
                total_feb REAL DEFAULT 0,
                promedio_u3 REAL DEFAULT 0,
                PRIMARY KEY (year_month, lanzamiento, cod_cliente, cod_vendedor)
            );

        """);
        # Indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fact_fact_ym ON fact_facturacion(year_month)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fact_avance_ym ON fact_avance_cliente_vendedor_month(year_month)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fact_fact_vendedor ON fact_facturacion(cod_vendedor)")
        self.conn.commit()

    def start_run(self):
        cursor = self.conn.cursor()
        cursor.execute("INSERT INTO etl_run (status) VALUES ('RUNNING')")
        self.run_id = cursor.lastrowid
        self.conn.commit()

    def end_run(self, status, message=""):
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE etl_run SET status = ?, message = ?, month_updated = ?, files_json = ? WHERE run_id = ?",
            (status, message, self.target_month, json.dumps(self.processed_files), self.run_id)
        )
        self.conn.commit()

    def find_file(self, pattern):
        files = list(self.data_dir.glob("*"))
        for f in files:
            if re.search(pattern, f.name, re.I):
                return f
        return None

    def process_dimensions(self):
        logging.info("Processing Dimensions...")
        
        # 1. Clients Master
        f_clients = self.find_file(REQUIRED_FILES['clients'])
        if f_clients:
            logging.info(f"Loading Clients Master from {f_clients.name}")
            df = robust_read_excel(f_clients, ["CLIENTEID", "CLIENTE"])
            df.columns = [str(c).strip().upper() for c in df.columns]
            
            for _, row in df.iterrows():
                cid = normalize_key(row.get('CLIENTEID'))
                if not cid: continue
                self.conn.execute("""
                    INSERT INTO dim_clients (cliente_id, cliente_name, cod_centralizador, frecuencia)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(cliente_id) DO UPDATE SET
                        cliente_name=excluded.cliente_name,
                        cod_centralizador=excluded.cod_centralizador,
                        frecuencia=excluded.frecuencia,
                        updated_at=CURRENT_TIMESTAMP
                """, (cid, row.get('CLIENTE'), normalize_key(row.get('COD CENTRALIZADOR')), row.get('FRECUENCIA')))
            self.processed_files.append(f_clients.name)

        # 2. Product Classification - Header is at row 6
        f_prod = self.find_file(REQUIRED_FILES['products'])
        if f_prod:
            logging.info(f"Loading Product Classification from {f_prod.name}")
            df = pd.read_excel(f_prod, header=6)
            df.columns = [str(c).strip().upper() for c in df.columns]
            logging.info(f"Product columns: {list(df.columns)[:10]}")
            
            for _, row in df.iterrows():
                pid = normalize_key(row.get('COD PRODUCTO'))
                if not pid: continue
                self.conn.execute("""
                    INSERT INTO dim_product_classification (cod_producto, descripcion, categoria, subcategoria)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(cod_producto) DO UPDATE SET
                        descripcion=excluded.descripcion,
                        categoria=excluded.categoria,
                        subcategoria=excluded.subcategoria,
                        updated_at=CURRENT_TIMESTAMP
                """, (pid, row.get('NOM PRODUCTO'), row.get('NOM CATEGORIA'), row.get('NOM CLASE COMERCIAL')))
            self.processed_files.append(f_prod.name)
        
        self.conn.commit()

    def process_facturacion(self):
        """Process all Facturación*.txt files (supports multiple monthly/daily files)."""
        # Search for all variations of facturacion files
        txt_patterns = [
            '*facturación*.txt', '*Facturación*.txt', '*facturacion*.txt',
            '*Factu*.txt', '*factu*.txt'  # Added for FactuNov25, FactuDic25, etc
        ]
        txt_files = []
        for pattern in txt_patterns:
            txt_files.extend(self.data_dir.glob(pattern))
        
        # Remove duplicates
        txt_files = list(set(txt_files))
        
        if not txt_files:
            logging.warning("No Facturación TXT files found. Skipping.")
            return
        
        logging.info(f"Found {len(txt_files)} Facturación file(s): {[f.name for f in txt_files]}")
        
        total_rows = 0
        for f_fact in sorted(txt_files):
            rows = self._process_single_facturacion(f_fact)
            total_rows += rows
        
        logging.info(f"Facturacion TOTAL: {total_rows} rows processed from {len(txt_files)} file(s)")

    def _process_single_facturacion(self, f_fact):
        """Process a single Facturación TXT file.
        Supports two formats:
        - Legacy: COD EMPRESA, FECHA EMISION, CANTIDAD KG, VALOR, COD CENTRALIZADOR
        - Minerva: COD_VENDEDOR, DTA_ENTRADA, QTD_KG_FATURADA, VAL_TOTAL_ITEM, COD_CENTRALIZADOR
        """
        logging.info(f"Processing Facturacion file: {f_fact.name}")

        try:
            df = pd.read_csv(f_fact, sep=';', encoding='latin-1', low_memory=False)
        except Exception:
            df = pd.read_csv(f_fact, sep=';', encoding='utf-8', low_memory=False)

        df.columns = [str(c).strip().upper() for c in df.columns]

        # --- Detect format ---
        is_minerva = 'COD_VENDEDOR' in df.columns and 'VAL_TOTAL_ITEM' in df.columns
        is_legacy  = any('EMPRESA' in col for col in df.columns[:5])

        if not is_minerva and not is_legacy:
            # Legacy format with missing headers
            logging.warning(f"  ⚠️  {f_fact.name} missing headers, adding standard headers")
            standard_headers = [
                'COD EMPRESA', 'NOM EMPRESA', 'NUM OFICIAL', 'FECHA EMISION', 'DIA', 'HORA', 'FECHA',
                'COD VENDEDOR', 'NOM VENDEDOR', 'COD CENTRALIZADOR', 'NOM CENTRALIZADOR',
                'COD CLIENTE', 'NOM CLIENTE', 'COD PRODUCTO VENTA', 'NOM PRODUCTO VENTA',
                'COD GRUPO COMERCIAL', 'NOM GRUPO COMERCIAL', 'COD SUBGRUPO COMERCIAL',
                'NOM SUBGRUPO COMERCIAL', 'COD CLASE COMERCIAL', 'NOM CLASE COMERCIAL',
                'COD FAMILIA COMERCIAL', 'NOM FAMILIA COMERCIAL', 'COD PRODUCTO DOCUMENTO',
                'NOM PRODUCTO DOCUMENTO', 'UNIDAD MEDIDA', 'CANTIDAD DOCUMENTO', 'CANTIDAD KG',
                'VALOR', 'VALOR NETO GRAVADO', 'MONEDA', 'COD TIPO NATUREZA', 'NOM TIPO NATUREZA',
                'ORIGEN DOCUMENTO', 'NOM TEMPLATE', 'PESO PADRON', 'NUM PEDIDO', 'COD.DEPOSITO',
                'NOMBRE DEPOSITO', 'VENCIMIENTO', 'DESC. CONDICION DE PAGO', 'CONDICION DE PAGO',
                'CANTIDAD KG BRUTO', 'PRECIO PEDIDO', 'PRECIO TABELA', 'COD. TABELA PRECO',
                'NOM. TABELA PRECO', 'ORIGEN PEDIDO', 'XCONTENT NRO PUNTO REMITO',
                'XCONTENT NRO OFICIAL REMITO', 'XCONTENT FECHA DE RENDICIÓN',
                'COD. TRANSPORTISTA', 'NOM. TRANSPORTISTA', 'UNNAMED: 53'
            ]
            df.columns = standard_headers[:len(df.columns)]
            is_legacy = True

        if is_minerva:
            logging.info(f"  → Detected MINERVA format for {f_fact.name}")
            return self._process_minerva_facturacion(df, f_fact.name)
        else:
            return self._process_legacy_facturacion(df, f_fact.name)

    def _process_legacy_facturacion(self, df, fname):
        """Insert rows from legacy format (COD EMPRESA, FECHA EMISION, CANTIDAD KG, VALOR)."""
        rows_inserted = 0
        for _, row in df.iterrows():
            raw_vals = [str(v) for v in row.values]
            row_hash = hashlib.md5("".join(raw_vals).encode()).hexdigest()

            fecha = row.get('FECHA EMISION')
            if pd.isna(fecha): continue

            try:
                dt = pd.to_datetime(fecha, dayfirst=True, errors='coerce')
                if pd.isna(dt): continue
                ym = dt.strftime('%Y-%m')
                iso_date = dt.strftime('%Y-%m-%d')
            except Exception:
                continue

            self.conn.execute("""
                INSERT OR IGNORE INTO fact_facturacion
                (row_hash, fecha_emision, cod_cliente, cod_vendedor, cod_producto, cantidad, importe, deposito, year_month)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row_hash, iso_date,
                normalize_key(row.get('COD CENTRALIZADOR')),
                normalize_key(row.get('COD VENDEDOR')),
                normalize_key(row.get('COD PRODUCTO VENTA')),
                coerce_numeric(row.get('CANTIDAD KG')),
                coerce_numeric(row.get('VALOR')),
                str(row.get('NOMBRE DEPOSITO', '')).strip(),
                ym
            ))
            rows_inserted += 1

        self.conn.commit()
        logging.info(f"  → {fname}: {rows_inserted} rows inserted/ignored (legacy)")
        self.processed_files.append(fname)
        return rows_inserted

    def _process_minerva_facturacion(self, df, fname):
        """Insert rows from Minerva format (COD_VENDEDOR, DTA_ENTRADA, QTD_KG_FATURADA, VAL_TOTAL_ITEM).
        Minerva files replace the full month — delete existing data first to avoid stale rows.
        """
        # Determine months present in the file to clear them first
        date_col = 'DTA_ENTRADA' if 'DTA_ENTRADA' in df.columns else 'DATA_EMISSAO'
        df['_dt'] = pd.to_datetime(df[date_col], dayfirst=True, errors='coerce')
        months_in_file = df['_dt'].dropna().dt.strftime('%Y-%m').unique()
        for ym in months_in_file:
            logging.info(f"  → MINERVA: clearing existing rows for {ym} before reload")
            self.conn.execute("DELETE FROM fact_facturacion WHERE year_month = ?", (ym,))
        self.conn.commit()

        rows_inserted = 0
        for _, row in df.iterrows():
            raw_vals = [str(v) for v in row.values]
            row_hash = hashlib.md5("".join(raw_vals).encode()).hexdigest()

            # Pick best date field
            dt = row.get('_dt')
            if pd.isna(dt): continue

            ym = dt.strftime('%Y-%m')
            iso_date = dt.strftime('%Y-%m-%d')

            cantidad = coerce_numeric(row.get('QTD_KG_FATURADA', 0))
            importe  = coerce_numeric(row.get('VAL_TOTAL_ITEM', 0))
            if importe == 0: continue  # skip lines with no value

            self.conn.execute("""
                INSERT OR IGNORE INTO fact_facturacion
                (row_hash, fecha_emision, cod_cliente, cod_vendedor, cod_producto, cantidad, importe, deposito, year_month)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row_hash, iso_date,
                normalize_key(row.get('COD_CENTRALIZADOR')),
                normalize_key(str(row.get('COD_VENDEDOR', ''))),
                normalize_key(str(row.get('COD_ITEM', ''))),
                cantidad,
                importe,
                str(row.get('DEPOSITO', '')).strip(),
                ym
            ))
            rows_inserted += 1

        self.conn.commit()
        logging.info(f"  → {fname}: {rows_inserted} rows inserted (minerva, full reload)")
        self.processed_files.append(fname)
        return rows_inserted


    def match_client(self, cod_cliente, nom_cliente, cod_centralizador):
        """Robust matching logic as per specs."""
        cursor = self.conn.cursor()
        
        # 1. Primary: ID Match
        cursor.execute("SELECT frecuencia FROM dim_clients WHERE cliente_id = ?", (cod_cliente,))
        res = cursor.fetchone()
        if res: return res[0], "id"

        # 2. Secondary: Centralizador Match
        if cod_centralizador:
            cursor.execute("SELECT frecuencia FROM dim_clients WHERE cod_centralizador = ? LIMIT 1", (cod_centralizador,))
            res = cursor.fetchone()
            if res: return res[0], "centralizador"

        # 3. Tertiary: Normalized Name Match
        norm_name = normalize_text(nom_cliente)
        if norm_name:
            cursor.execute("SELECT cliente_id, frecuencia, cliente_name FROM dim_clients")
            all_clients = cursor.fetchall()
            matches = [c for c in all_clients if normalize_text(c[2]) == norm_name]
            
            if len(matches) == 1:
                return matches[0][1], "name"
            elif len(matches) > 1:
                logging.warning(f"AMBIGUOUS_NAME_MATCH for {nom_cliente} ({cod_cliente}). Picking first.")
                return matches[0][1], "name"

        return None, "unmatched"

    def process_avance_vendedor(self):
        f_path = self.find_file(REQUIRED_FILES['avance_vendedor'])
        if not f_path:
            logging.error("Avance x Cliente-Vendedor not found!")
            return

        _, ym = parse_date_from_filename(f_path.name, self.year)
        if not ym: raise ValueError("Could not determine month from filename.")
        
        self.target_month = ym
        logging.info(f"Target Month detected: {self.target_month} for file {f_path.name}")

        # Header is at row 1
        df = pd.read_excel(f_path, header=1)
        df.columns = [str(c).strip().upper() for c in df.columns]
        logging.info(f"Avance columns: {list(df.columns)[:15]}")
        
        self.conn.execute("DELETE FROM fact_avance_cliente_vendedor_month WHERE year_month = ?", (self.target_month,))
        
        rows_data = []
        unmatched_count = 0
        
        # Detect sales column dynamically: FEB '25, ENE '25, etc. or FACTURACIÓN
        month_abbr = datetime.strptime(ym, '%Y-%m').strftime('%b').upper()[:3]
        year_short = ym[2:4]
        month_patterns = [
            f"{month_abbr} '{year_short}",  # "FEB '25"
            f"{month_abbr}'{year_short}",   # "FEB'25"
            "FACTURACIÓN",
            "FACTURACION"
        ]
        
        sales_col = None
        for col in df.columns:
            for pattern in month_patterns:
                if pattern in col.upper():
                    sales_col = col
                    break
            if sales_col:
                break
        
        logging.info(f"Using sales column: {sales_col}")

        for _, row in df.iterrows():
            c_id = normalize_key(row.get('COD CENTRALIZADOR'))
            c_name = row.get('NOM CENTRALIZADOR')
            
            if not c_id or pd.isna(c_id): continue
            if c_name and "TOTAL" in str(c_name).upper(): continue
            
            frec, quality = self.match_client(c_id, c_name, c_id)
            
            if quality == "unmatched":
                unmatched_count += 1
                self.conn.execute("""
                    INSERT INTO etl_unmatched_clients (run_id, year_month, cod_cliente, nom_cliente, cod_centralizador, reason)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (self.run_id, self.target_month, c_id, c_name, c_id, "No match found in Clientes_master"))

            rows_data.append((
                self.target_month,
                str(row.get('CANAL', '')).strip(),
                str(row.get('ZONA', '')).strip(),
                str(row.get('JEFE', '')).strip(),
                normalize_key(row.get('COD VENDEDOR')),
                row.get('NOM VENDEDOR'),
                c_id,
                c_name,
                c_id,
                coerce_numeric(row.get(sales_col) if sales_col else 0),
                coerce_numeric(row.get('OBJETIVO')),
                coerce_numeric(row.get('PENDIENTE')),
                frec,
                quality
            ))

        self.conn.executemany("""
            INSERT INTO fact_avance_cliente_vendedor_month 
            (year_month, canal, zona, jefe, cod_vendedor, nom_vendedor, cod_cliente, nom_cliente, cod_centralizador, venta_actual, objetivo, pendiente, frecuencia, match_quality)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows_data)
        
        self.conn.commit()
        logging.info(f"Avance Vendedor processed: {len(rows_data)} rows. Unmatched clients: {unmatched_count}")
        self.processed_files.append(f_path.name)
        
        # Now extract historical months
        self.process_cliente_historico(df)
        
    def sync_facturacion_to_avance(self):

        """Update venta_actual in fact_avance_cliente_vendedor_month using the real 
        sum of KG from fact_facturacion for the current target month.
        This fixes discrepancies where the Avance Excel is late compared to the TXT.
        """
        ym = self.target_month
        logging.info(f"Syncing Facturacion KG to Avance table for {ym}")
        
        # Calculate real KG sum per (cod_vendedor, cod_cliente) from fact_facturacion
        # Note: we group by cod_cliente only because in theory it's assigned to one vendor 
        # but to be safe we match the specific vendor-client pair.
        self.conn.execute("""
            WITH real_sales AS (
                SELECT cod_cliente, cod_vendedor, SUM(cantidad) as total_kg
                FROM fact_facturacion
                WHERE year_month = ?
                GROUP BY cod_cliente, cod_vendedor
            )
            UPDATE fact_avance_cliente_vendedor_month
            SET venta_actual = (
                SELECT total_kg FROM real_sales 
                WHERE real_sales.cod_cliente = fact_avance_cliente_vendedor_month.cod_cliente
                  AND real_sales.cod_vendedor = fact_avance_cliente_vendedor_month.cod_vendedor
            )
            WHERE year_month = ?
              AND EXISTS (
                SELECT 1 FROM real_sales 
                WHERE real_sales.cod_cliente = fact_avance_cliente_vendedor_month.cod_cliente
                  AND real_sales.cod_vendedor = fact_avance_cliente_vendedor_month.cod_vendedor
              )
        """, (ym, ym))
        
        self.conn.commit()
        logging.info("Facturacion KG synced to Avance table.")

    def process_cliente_historico(self, df):

        """Extract historical monthly sales from Avance x Cliente-Vendedor columns."""
        import re
        
        # Find all month columns (patterns like SEP '24, OCT '24, ENE '25, etc.)
        month_pattern = re.compile(r"^(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|SEPT|OCT|NOV|DIC)\s*['\"]?\s*(\d{2})$", re.IGNORECASE)
        
        month_cols = []
        for col in df.columns:
            col_upper = str(col).strip().upper()
            match = month_pattern.match(col_upper)
            if match:
                month_name = match.group(1)
                year_short = match.group(2)
                # Convert to year_month format
                month_map = {'ENE': '01', 'FEB': '02', 'MAR': '03', 'ABR': '04', 'MAY': '05', 'JUN': '06',
                             'JUL': '07', 'AGO': '08', 'SEP': '09', 'SEPT': '09', 'OCT': '10', 'NOV': '11', 'DIC': '12'}
                month_num = month_map.get(month_name[:3], '01')
                year_full = '20' + year_short
                year_month = f"{year_full}-{month_num}"
                month_cols.append((col, year_month))
        
        logging.info(f"Found {len(month_cols)} historical month columns: {[ym for _, ym in month_cols]}")
        
        # Clear existing historical data
        self.conn.execute("DELETE FROM fact_cliente_historico")
        
        hist_rows = []
        for _, row in df.iterrows():
            c_id = normalize_key(row.get('COD CENTRALIZADOR'))
            v_id = normalize_key(row.get('COD VENDEDOR'))
            
            if not c_id or pd.isna(c_id): continue
            if str(row.get('NOM CENTRALIZADOR', '')).upper().startswith('TOTAL'): continue
            
            for col_name, year_month in month_cols:
                kg = coerce_numeric(row.get(col_name))
                if kg and kg > 0:
                    hist_rows.append((c_id, v_id, year_month, kg))
        
        self.conn.executemany("""
            INSERT OR REPLACE INTO fact_cliente_historico (cod_cliente, cod_vendedor, year_month, kg_vendidos)
            VALUES (?, ?, ?, ?)
        """, hist_rows)
        
        self.conn.commit()
        logging.info(f"Client historical data: {len(hist_rows)} month-records extracted")

    # Note: Avance General and Stock vs Pendiente would follow similar patterns.
    # For MVP, focusing on the Primary/Authoritative guide as requested.

    def update_premium_flag(self):
        """Mark products in fact_facturacion as premium based on dim_product_classification."""
        cursor = self.conn.cursor()
        
        # Update es_premium based on subcategoria = 'PREMIUM'
        cursor.execute("""
            UPDATE fact_facturacion
            SET es_premium = 1
            WHERE cod_producto IN (
                SELECT cod_producto FROM dim_product_classification WHERE subcategoria = 'PREMIUM'
            )
        """)
        
        updated = cursor.rowcount
        self.conn.commit()
        logging.info(f"Premium flag updated: {updated} rows marked as premium")

    def process_category_sheets(self):
        """Process all category sheets to extract facturacion_pesos per client."""
        f_path = self.find_file(REQUIRED_FILES['avance_vendedor'])
        if not f_path:
            logging.warning("Avance x Cliente-Vendedor not found for category processing!")
            return
        
        # Category sheets to process
        category_sheets = ['HB', 'SCH', 'UNT', 'RB', 'SJ', 'GRASA', 'PICADA', 'CHORIZOS', 'PAPAS', 'ATUN', 'CORTES CARNE']
        
        # Dictionary to accumulate facturacion by (cod_cliente, cod_vendedor)
        client_facturacion = {}
        
        for sheet in category_sheets:
            try:
                df = pd.read_excel(f_path, sheet_name=sheet, header=2)
                logging.info(f"Processing sheet: {sheet}")
                
                # Find FACTURACIÓN column (should be column AA, index 26)
                fact_col = None
                for col in df.columns:
                    if str(col).strip().upper() == 'FACTURACIÓN':
                        fact_col = col
                        break
                
                if not fact_col:
                    logging.warning(f"  FACTURACIÓN column not found in sheet {sheet}, skipping")
                    continue
                
                for _, row in df.iterrows():
                    cod_cli = normalize_key(row.get('COD CENTRALIZADOR'))
                    cod_ven = normalize_key(row.get('COD VENDEDOR'))
                    facturacion = coerce_numeric(row.get(fact_col))
                    
                    if cod_cli and cod_ven and facturacion > 0:
                        key = (cod_cli, cod_ven)
                        client_facturacion[key] = client_facturacion.get(key, 0) + facturacion
                
                logging.info(f"  {sheet}: Processed {len(df)} rows")
            
            except Exception as e:
                logging.warning(f"  Failed to process sheet {sheet}: {e}")
                continue
        
        # Update fact_avance_cliente_vendedor_month with facturacion_pesos
        cursor = self.conn.cursor()
        updates = 0
        
        for (cod_cli, cod_ven), facturacion_total in client_facturacion.items():
            cursor.execute("""
                UPDATE fact_avance_cliente_vendedor_month
                SET facturacion_pesos = ?
                WHERE cod_cliente = ? AND cod_vendedor = ? AND year_month = ?
            """, (facturacion_total, cod_cli, cod_ven, self.target_month))
            updates += cursor.rowcount
        
        self.conn.commit()
        logging.info(f"Category sheets: Updated {updates} client records with facturacion_pesos")
        
        # Now calculate objetivos_pesos proportionally
        self.calculate_monetary_objectives()

    def calculate_monetary_objectives(self):
        """Calculate objetivo_pesos and objetivo_premium_pesos based on vendedor_objetivos."""
        cursor = self.conn.cursor()
        
        # Get all vendors with objectives
        vendors = cursor.execute("""
            SELECT cod_vendedor, objetivo_pesos, objetivo_premium_pesos, objetivo_kg
            FROM vendedor_objetivos
        """).fetchall()
        
        updates = 0
        for vendor in vendors:
            cod_ven, obj_pesos, obj_premium, obj_kg = vendor
            
            if not obj_kg or obj_kg == 0:
                continue
            
            # Get all clients for this vendor
            clients = cursor.execute("""
                SELECT cod_cliente, objetivo
                FROM fact_avance_cliente_vendedor_month
                WHERE cod_vendedor = ? AND year_month = ?
            """, (cod_ven, self.target_month)).fetchall()
            
            for cod_cli, cliente_kg in clients:
                if not cliente_kg or cliente_kg == 0:
                    continue
                
                # Proportional allocation
                proportion = cliente_kg / obj_kg
                cliente_obj_pesos = (obj_pesos or 0) * proportion
                cliente_obj_premium = (obj_premium or 0) * proportion
                
                cursor.execute("""
                    UPDATE fact_avance_cliente_vendedor_month
                    SET objetivo_pesos = ?, objetivo_premium_pesos = ?
                    WHERE cod_cliente = ? AND cod_vendedor = ? AND year_month = ?
                """, (cliente_obj_pesos, cliente_obj_premium, cod_cli, cod_ven, self.target_month))
                updates += cursor.rowcount
        
        self.conn.commit()
        logging.info(f"Calculated monetary objectives for {updates} client records")

    def seed_objetivos(self):
        """Seed vendedor objectives from objetivos data."""
        cursor = self.conn.cursor()

        # Read existing rebozados goal if already set (check both id formats)
        existing = cursor.execute(
            "SELECT objetivo_rebozados_kg FROM vendedor_objetivos WHERE cod_vendedor IN ('100067806','100067806.0')"
        ).fetchone()
        reb_kg = existing[0] if existing and existing[0] else 8000

        # Upsert with normalized ID (no .0)
        cursor.execute("""
            INSERT OR REPLACE INTO vendedor_objetivos
            (cod_vendedor, nom_vendedor, year_month, objetivo_pesos, objetivo_premium_pesos, objetivo_kg, objetivo_rebozados_kg)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, ('100067806', 'GENTILE NICOLAS', '2026-02', 499163842, 55050592, 76942, reb_kg))

        self.conn.commit()
        logging.info(f"Seeded objectives for GENTILE NICOLAS (Feb 2026) rebozados_kg={reb_kg}")

    def apply_vendor_aliases(self):
        """Unify vendor codes: remap Perotti codes to Gentile. Also normalize .0 suffix."""
        # First normalize all .0 suffixes in fact_facturacion cod_vendedor
        self.conn.execute("""
            UPDATE fact_facturacion
            SET cod_vendedor = SUBSTR(cod_vendedor, 1, LENGTH(cod_vendedor)-2)
            WHERE cod_vendedor LIKE '%.0' AND CAST(SUBSTR(cod_vendedor, 1, LENGTH(cod_vendedor)-2) AS INTEGER) > 0
        """)
        self.conn.commit()
        logging.info("Normalized .0 suffix in fact_facturacion cod_vendedor")

        # Then remap aliases (Perotti, Vacante Santa Fe → Gentile)
        aliases = [
            ('100075864', '100067806', 'GENTILE NICOLAS'),
            ('100075865', '100067806', 'GENTILE NICOLAS'),
            ('100089597', '100067806', 'GENTILE NICOLAS'),  # VACANTE SANTA FE IND
        ]
        total_avance = 0
        total_fact   = 0
        for src, dst, dst_name in aliases:
            cur = self.conn.execute(
                "UPDATE fact_avance_cliente_vendedor_month SET cod_vendedor=?, nom_vendedor=? WHERE cod_vendedor IN (?,?)",
                (dst, dst_name, src, src + '.0')
            )
            total_avance += cur.rowcount
            cur2 = self.conn.execute(
                "UPDATE fact_facturacion SET cod_vendedor=? WHERE cod_vendedor IN (?,?)",
                (dst, src, src + '.0')
            )
            total_fact += cur2.rowcount
        self.conn.commit()
        logging.info(f"Vendor aliases: {total_avance} avance, {total_fact} facturacion rows remapped to Gentile")



    def process_lanzamientos(self):
        """Process Compradores Lanzamientos.xlsx — one sheet per launch product.
        Imports:
          - Current month (FEB '26): full estado + fact/pend/total/promedio
          - Historical months (ENE '26, DIC '25, etc.): kg per client per month
        """
        f_path = self.find_file(r'Compradores Lanzamientos.*\.xlsx')
        if not f_path:
            logging.warning("Compradores Lanzamientos not found, skipping.")
            return

        logging.info(f"Processing Lanzamientos from {f_path.name}")
        xls = pd.ExcelFile(f_path)

        # Sheets to process (skip summary/dynamic sheets)
        skip = {'DINAMICA ENERO 26', 'DINAMICA'}
        sheets = [s for s in xls.sheet_names if s.upper() not in {x.upper() for x in skip}]

        # Clear existing data for target month only
        ym = self.target_month or f'{self.year}-{datetime.now().month:02d}'
        self.conn.execute("DELETE FROM fact_lanzamiento_cobertura WHERE year_month = ?", (ym,))

        # Month name → number map for historical col parsing
        _MONTH_MAP = {
            'ENE': '01', 'FEB': '02', 'MAR': '03', 'ABR': '04',
            'MAY': '05', 'JUN': '06', 'JUL': '07', 'AGO': '08',
            'SEP': '09', 'SEPT': '09', 'OCT': '10', 'NOV': '11', 'DIC': '12'
        }
        _HIST_COL_RE = re.compile(
            r"^(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|SEPT|OCT|NOV|DIC)['\s]*(\d{2})$",
            re.IGNORECASE
        )

        def col_to_ym(col_name):
            """Convert a column header like \"ENE '26\" or \"DIC 25\" to \"2026-01\"."""
            m = _HIST_COL_RE.match(str(col_name).strip().upper().replace("'", "").replace(" ", ""))
            if not m:
                return None
            mon = _MONTH_MAP.get(m.group(1)[:3])
            yr = '20' + m.group(2)
            return f"{yr}-{mon}" if mon else None

        total = 0
        hist_total = 0

        for sheet in sheets:
            try:
                df = pd.read_excel(xls, sheet_name=sheet, header=1)
                df.columns = [str(c).strip() for c in df.columns]

                # ── Current month: ESTADO + fact/pend/total/promedio ─────────
                if 'ESTADO' not in df.columns:
                    logging.warning(f"  {sheet}: no ESTADO column, skipping current month")
                else:
                    # Map month number to abbreviation for Excel column search (e.g., '02' -> 'FEB')
                    # This fixes the issue where Feb columns like "FEB '26 FACT" were not found
                    rev_month_map = {v: k for k, v in _MONTH_MAP.items()}
                    month_abbr = rev_month_map.get(ym[-2:])
                    
                    fact_col  = next((c for c in df.columns if 'FACT'    in c.upper() and month_abbr in c.upper()), None)
                    pend_col  = next((c for c in df.columns if 'PEND'    in c.upper() and month_abbr in c.upper()), None)
                    total_col = next((c for c in df.columns if 'TOTAL'   in c.upper() and month_abbr in c.upper()), None)
                    prom_col  = next((c for c in df.columns if 'PROMEDIO' in c.upper()), None)


                    rows_cur = []
                    for _, row in df.iterrows():
                        cod_cli = normalize_key(row.get('COD CENTRALIZADOR'))
                        if not cod_cli or pd.isna(cod_cli): continue
                        nom = row.get('NOM CENTRALIZADOR', '')
                        if pd.notna(nom) and 'TOTAL' in str(nom).upper(): continue

                        estado = str(row.get('ESTADO', '')).strip()
                        if 'COMPRADOR' in estado.upper() and 'SIN' not in estado.upper() and 'NO' not in estado.upper():
                            estado_norm = 'COMPRADOR'
                        elif 'SIN COMPRA' in estado.upper():
                            estado_norm = 'SIN COMPRA'
                        elif 'NO COMPRADOR' in estado.upper():
                            estado_norm = 'NO COMPRADOR'
                        else:
                            estado_norm = estado or 'DESCONOCIDO'

                        rows_cur.append((
                            ym, sheet,
                            normalize_key(row.get('COD VENDEDOR')),
                            str(row.get('NOM VENDEDOR', '')).strip(),
                            cod_cli,
                            str(nom).strip(),
                            str(row.get('CANAL', '')).strip(),
                            str(row.get('ZONA', '')).strip(),
                            estado_norm,
                            coerce_numeric(row.get(fact_col))  if fact_col  else 0,
                            coerce_numeric(row.get(pend_col))  if pend_col  else 0,
                            coerce_numeric(row.get(total_col)) if total_col else 0,
                            coerce_numeric(row.get(prom_col))  if prom_col  else 0,
                        ))

                    self.conn.executemany("""
                        INSERT OR REPLACE INTO fact_lanzamiento_cobertura
                        (year_month, lanzamiento, cod_vendedor, nom_vendedor, cod_cliente, nom_cliente,
                         canal, zona, estado, fact_feb, pend_feb, total_feb, promedio_u3)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, rows_cur)
                    total += len(rows_cur)
                    logging.info(f"  {sheet}: {len(rows_cur)} rows")

                # ── Historical months: one row per (ym_hist, sheet, cod_cliente) ────
                hist_cols = []
                for col in df.columns:
                    ym_hist = col_to_ym(col)
                    if ym_hist and ym_hist != ym:
                        hist_cols.append((col, ym_hist))

                rows_hist = []
                for _, row in df.iterrows():
                    cod_cli = normalize_key(row.get('COD CENTRALIZADOR'))
                    if not cod_cli or pd.isna(cod_cli): continue
                    nom = row.get('NOM CENTRALIZADOR', '')
                    if pd.notna(nom) and 'TOTAL' in str(nom).upper(): continue

                    cod_ven = normalize_key(row.get('COD VENDEDOR'))
                    nom_ven = str(row.get('NOM VENDEDOR', '')).strip()
                    canal   = str(row.get('CANAL', '')).strip()
                    zona    = str(row.get('ZONA', '')).strip()

                    for col, ym_hist in hist_cols:
                        kg = coerce_numeric(row.get(col))
                        if not kg or kg <= 0:
                            continue
                        rows_hist.append((
                            ym_hist, sheet,
                            cod_ven, nom_ven,
                            cod_cli, str(nom).strip(),
                            canal, zona,
                            'HISTORIAL',       # estado
                            kg,                # fact_feb = kg ese mes
                            0, kg, 0,          # pend=0, total=kg, prom=0
                        ))

                if rows_hist:
                    # Only clear the specific hist months we're about to write
                    hist_months = set(r[0] for r in rows_hist)
                    for hm in hist_months:
                        self.conn.execute(
                            "DELETE FROM fact_lanzamiento_cobertura WHERE year_month = ? AND lanzamiento = ?",
                            (hm, sheet)
                        )
                    self.conn.executemany("""
                        INSERT OR REPLACE INTO fact_lanzamiento_cobertura
                        (year_month, lanzamiento, cod_vendedor, nom_vendedor, cod_cliente, nom_cliente,
                         canal, zona, estado, fact_feb, pend_feb, total_feb, promedio_u3)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, rows_hist)
                    hist_total += len(rows_hist)

            except Exception as e:
                logging.warning(f"  {sheet}: FAILED — {e}")
                continue

        self.conn.commit()
        self.processed_files.append(f_path.name)
        logging.info(f"Lanzamientos TOTAL: {total} rows (current month) + {hist_total} rows (historical) across {len(sheets)} products")



    def run_all(self):
        try:
            self.init_db()
            self.start_run()
            
            self.process_dimensions()
            self.process_facturacion()
            self.process_avance_vendedor()
            self.apply_vendor_aliases()   # Perotti → Gentile auto
            self.sync_facturacion_to_avance() # Sync TXT KG to Avance table
            self.update_premium_flag()
            self.seed_objetivos()
            self.process_category_sheets()
            self.process_lanzamientos()
            
            self.end_run("SUCCESS", "ETL completed successfully.")
            logging.info("--- ETL SUMMARY ---")
            logging.info(f"Run ID: {self.run_id}")
            logging.info(f"Month Updated: {self.target_month}")
            logging.info(f"Files Processed: {len(self.processed_files)}")
            
        except Exception as e:
            logging.error(f"ETL FAILED: {str(e)}")
            self.end_run("FAILED", str(e))
            raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sales Ops ETL Engine")
    parser.add_argument("--data-dir", default="data", help="Directory with source files")
    parser.add_argument("--db-path", default="db/app.db", help="Target SQLite DB path")
    parser.add_argument("--log-path", default="logs/etl.log", help="Log file path")
    parser.add_argument("--year", type=int, help="Override year for month detection")
    parser.add_argument("--export-json", action="store_true", help="Export JSON files after ETL")
    
    args = parser.parse_args()
    
    setup_logging(args.log_path)
    
    etl = SalesETL(args.data_dir, args.db_path, args.year)
    etl.run_all()
    
    # Export JSON if requested
    if args.export_json:
        logging.info("=== Starting JSON Export ===")
        try:
            from export_json import JSONExporter
            exporter = JSONExporter(args.db_path, args.data_dir)
            totals = exporter.export_all()
            logging.info(f"JSON export completed: {totals}")
        except Exception as e:
            logging.error(f"JSON export failed: {str(e)}")
            raise

