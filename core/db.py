import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / 'db' / 'app.db'

def get_db():
    """Get database connection with row factory."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # Ensure the manual-payment tracking table exists (lightweight, idempotent)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fact_factura_pagada (
            cod_cliente   TEXT NOT NULL,
            fecha_emision TEXT NOT NULL,
            marked_at     TEXT NOT NULL DEFAULT (datetime('now')),
            marked_by     TEXT,
            PRIMARY KEY (cod_cliente, fecha_emision)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crm_alertas_dismissed (
            alert_id TEXT PRIMARY KEY,
            dismissed_at TEXT DEFAULT (datetime('now')),
            user_id TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crm_planificacion_recurrente (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cod_cliente TEXT,
            descripcion TEXT,
            dia_semana INTEGER,
            activo INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crm_cliente_ponderacion (
            cod_cliente TEXT,
            year_month TEXT,
            ponderacion_pct REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (cod_cliente, year_month)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crm_planificacion_recurrente_completado (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recurrente_id INTEGER,
            fecha DATE,
            completado INTEGER DEFAULT 1,
            resultado TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(recurrente_id, fecha)
        )
    """)
    conn.commit()
    return conn
