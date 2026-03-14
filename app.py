#!/usr/bin/env python3
"""
Sales Dashboard - Flask Backend
Author: Archi (Antigravity)

Run: python app.py
Access: http://localhost:5000
"""

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from functools import wraps
from flask import Flask, render_template, jsonify, request, redirect, url_for, session
import calendar

app = Flask(__name__, template_folder='templates', static_folder='assets', static_url_path='/static')
app.secret_key = 'sales_dashboard_secret_key_change_in_production'

DB_PATH = Path(__file__).parent / 'db' / 'app.db'


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


def login_required(f):
    """Decorator for routes that require login."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function


# ==================== PAGES ====================

@app.route('/')
def index():
    """Redirect to login or filters."""
    if 'user' in session:
        return redirect(url_for('filters'))
    return redirect(url_for('login'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    """Simple login page."""
    if request.method == 'POST':
        # Simple auth - in production use proper auth
        username = request.form.get('username', '')
        if username:
            session['user'] = username
            return redirect(url_for('filters'))
    return render_template('login.html')


@app.route('/logout')
def logout():
    """Logout and clear session."""
    session.clear()
    return redirect(url_for('login'))


@app.route('/filters')
@login_required
def filters():
    """Filter selection page."""
    return render_template('filters.html')


@app.route('/dashboard')
@login_required
def dashboard():
    """Main dashboard page."""
    vendedor = request.args.get('vendedor', '')
    return render_template('dashboard.html', vendedor=vendedor)


@app.route('/cliente/<cod_cliente>')
@login_required
def cliente_detail(cod_cliente):
    """Client detail page."""
    return render_template('cliente.html', cod_cliente=cod_cliente)


@app.route('/crm')
@login_required
def crm():
    """CRM Dashboard page."""
    return render_template('crm.html')


@app.route('/pricing')
@login_required
def pricing():
    """Pricing and BI page."""
    return render_template('pricing.html')


# ==================== API ENDPOINTS ====================


@app.route('/api/welcome')
def api_welcome():
    """
    Returns welcome data for the dashboard modal: KPIs, alertas, consejos contextuales.
    Uses same filters as dashboard (vendedor, jefe, zona).
    """
    vendedor = request.args.get('vendedor', '')
    jefe = request.args.get('jefe', '')
    zona = request.args.get('zona', '')

    conn = get_db()
    where_parts, params = [], []
    if vendedor:
        where_parts.append("av.cod_vendedor = ?")
        params.append(vendedor)
    elif jefe:
        where_parts.append("av.jefe = ?")
        params.append(jefe)
    elif zona:
        where_parts.append("av.zona = ?")
        params.append(zona)
    where_clause = " AND ".join(where_parts) if where_parts else "1=1"
    params_kpi = params.copy()

    kpi_q = f"""
        SELECT SUM(av.venta_actual) as venta_kg, SUM(av.objetivo) as objetivo_kg, SUM(av.objetivo_pesos) as objetivo_pesos
        FROM fact_avance_cliente_vendedor_month av
        WHERE {where_clause}
          AND av.year_month = (SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month)
    """
    kpi_row = conn.execute(kpi_q, params_kpi).fetchone() if where_parts else conn.execute(kpi_q).fetchone()

    venta_kg = kpi_row['venta_kg'] or 0
    obj_kg = kpi_row['objetivo_kg'] or 0
    obj_pesos = kpi_row['objetivo_pesos'] or 0
    cumplimiento = round((venta_kg / obj_kg * 100), 1) if obj_kg else 0

    # Alertas
    from datetime import datetime
    hoy = datetime.now()
    mapping = {0: 'LUNES', 1: 'MARTES', 2: 'MIERCOLES', 3: 'JUEVES', 4: 'VIERNES', 5: 'SABADO', 6: 'DOMINGO'}
    freq_map = {0: ['MARTES', 'MIERCOLES'], 1: ['MIERCOLES', 'JUEVES'], 2: ['JUEVES', 'VIERNES'],
                3: ['VIERNES', 'LUNES'], 4: ['LUNES', 'MARTES'], 5: [], 6: []}
    target_freqs = freq_map.get(hoy.weekday(), [])
    mañana = hoy + timedelta(days=1)
    dia_manana = mapping.get(mañana.weekday())

    alertas_deuda_hoy = 0
    if target_freqs:
        freq_cond = " OR ".join(["UPPER(av.frecuencia) LIKE ?" for _ in target_freqs])
        params_ah = (params + [f"%{f}%" for f in target_freqs]) if where_parts else [f"%{f}%" for f in target_freqs]
        clientes_hoy = conn.execute(f"""
            SELECT av.cod_cliente, c.plazo, av.frecuencia
            FROM fact_avance_cliente_vendedor_month av
            JOIN dim_clients c ON av.cod_cliente = c.cliente_id
            WHERE {where_clause} AND ({freq_cond})
        """, params_ah).fetchall()
        for cl in clientes_hoy:
            plazo = str(cl['plazo'] or '').strip().lower()
            if plazo == 'anticipado':
                alertas_deuda_hoy += 1
            elif plazo and plazo.isdigit():
                deuda = conn.execute("""
                    SELECT 1 FROM fact_facturacion
                    WHERE cod_cliente = ? AND cantidad > 0 AND fecha_emision >= date('now', '-90 day')
                      AND julianday('now') - julianday(fecha_emision) > ?
                    LIMIT 1
                """, (cl['cod_cliente'], int(plazo))).fetchone()
                if deuda:
                    alertas_deuda_hoy += 1

    alertas_deuda_manana = 0
    if dia_manana:
        if where_parts:
            params_manana = params + [f"%{dia_manana}%"]
            clientes_manana = conn.execute(f"""
                SELECT av.cod_cliente, c.plazo
                FROM fact_avance_cliente_vendedor_month av
                JOIN dim_clients c ON av.cod_cliente = c.cliente_id
                WHERE {where_clause} AND UPPER(av.frecuencia) LIKE ?
            """, params_manana).fetchall()
        else:
            clientes_manana = conn.execute("""
                SELECT av.cod_cliente, c.plazo
                FROM fact_avance_cliente_vendedor_month av
                JOIN dim_clients c ON av.cod_cliente = c.cliente_id
                WHERE av.year_month = (SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month)
                  AND UPPER(av.frecuencia) LIKE ?
            """, [f"%{dia_manana}%"]).fetchall()
        for cl in clientes_manana:
            plazo = str(cl['plazo'] or '').strip().lower()
            if plazo == 'anticipado':
                alertas_deuda_manana += 1
            elif plazo and plazo.isdigit():
                deuda = conn.execute("""
                    SELECT 1 FROM fact_facturacion
                    WHERE cod_cliente = ? AND cantidad > 0 AND fecha_emision >= date('now', '-90 day')
                      AND julianday('now') - julianday(fecha_emision) > ?
                    LIMIT 1
                """, (cl['cod_cliente'], int(plazo))).fetchone()
                if deuda:
                    alertas_deuda_manana += 1

    gestiones_pend = conn.execute("""
        SELECT COUNT(*) as n FROM crm_gestiones g
        WHERE proximo_paso_fecha <= date('now', '+2 days') AND proximo_paso_fecha IS NOT NULL
    """).fetchone()['n'] or 0

    # Gestiones recurrentes pendientes para hoy
    hoy_sql = hoy.strftime('%Y-%m-%d')
    recurr_pend = conn.execute("""
        SELECT COUNT(*) as n FROM crm_planificacion_recurrente r
        LEFT JOIN crm_planificacion_recurrente_completado c ON c.recurrente_id = r.id AND c.fecha = ?
        WHERE r.activo = 1 AND r.dia_semana = ? AND (c.completado IS NULL OR c.completado = 0)
    """, (hoy_sql, hoy.weekday())).fetchone()['n'] or 0

    # Consejos contextuales (rule-based)
    consejos = []
    if alertas_deuda_hoy: consejos.append(f"Tenés {alertas_deuda_hoy} cliente(s) con deuda o contado anticipado para gestionar hoy. Priorizá cobranzas.")
    if alertas_deuda_manana: consejos.append(f"Mañana {alertas_deuda_manana} cliente(s) con deuda o contado anticipado. Revisá el plan de visitas.")
    if gestiones_pend: consejos.append(f"{gestiones_pend} gestión(es) con próximo paso vencido. Revisá el CRM.")
    if recurr_pend: consejos.append(f"Tenés {recurr_pend} tarea(s) programada(s) para hoy (ej. cargar pedidos). Revisá Planificación.")
    if cumplimiento < 60 and obj_kg: consejos.append(f"El objetivo del mes está al {cumplimiento}%. Enfocate en clientes con bajo avance.")
    elif cumplimiento >= 80 and obj_kg: consejos.append(f"¡Buen avance! El mes va al {cumplimiento}% del objetivo.")
    if not consejos: consejos.append("No hay alertas críticas. Revisá el plan del día en el CRM.")

    mes_row = conn.execute("SELECT MAX(year_month) as ym FROM fact_avance_cliente_vendedor_month").fetchone()
    mes_activo = mes_row['ym'] if mes_row else None
    if mes_activo:
        y, m = mes_activo.split('-')
        meses_es = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']
        mes_label = f"{meses_es[int(m) - 1]} {y}"
    else:
        mes_label = "Mes actual"

    conn.close()

    return jsonify({
        'mes_activo': mes_activo,
        'mes_label': mes_label,
        'kpis': {
            'venta_kg': round(venta_kg, 0),
            'objetivo_kg': round(obj_kg, 0),
            'objetivo_pesos': round(obj_pesos, 0),
            'cumplimiento_pct': cumplimiento,
        },
        'alertas': {
            'deuda_hoy': alertas_deuda_hoy,
            'deuda_manana': alertas_deuda_manana,
            'gestiones_pendientes': gestiones_pend,
            'gestiones_recurrentes_hoy': recurr_pend,
            'total': alertas_deuda_hoy + alertas_deuda_manana + gestiones_pend + recurr_pend,
        },
        'consejos': consejos,
    })


@app.route('/api/meta')
def api_meta():
    """Return metadata: last data load date and active month."""
    conn = get_db()
    row = conn.execute("""
        SELECT
            MAX(year_month)     as mes_activo,
            MAX(fecha_emision)  as ultima_fecha_fact
        FROM fact_facturacion
    """).fetchone()
    conn.close()

    mes_activo = row['mes_activo'] if row else None
    ultima_fecha = row['ultima_fecha_fact'] if row else None

    # Format as "26 feb 2026" if date present
    label = None
    if ultima_fecha:
        try:
            dt = datetime.strptime(str(ultima_fecha)[:10], '%Y-%m-%d')
            meses_es = ['ene','feb','mar','abr','may','jun','jul','ago','sep','oct','nov','dic']
            label = f"{dt.day} {meses_es[dt.month-1]} {dt.year}"
        except Exception:
            label = str(ultima_fecha)[:10]

    return jsonify({
        'mes_activo': mes_activo,
        'ultima_fecha_fact': ultima_fecha,
        'ultima_carga_label': label
    })

@app.route('/api/filters')
def api_filters():
    """Return hierarchy data for cascading filters."""
    conn = get_db()
    
    # Get distinct values
    zonas = conn.execute("""
        SELECT DISTINCT zona FROM fact_avance_cliente_vendedor_month 
        WHERE zona IS NOT NULL AND zona != '' 
        ORDER BY zona
    """).fetchall()
    
    jefes = conn.execute("""
        SELECT DISTINCT zona, jefe FROM fact_avance_cliente_vendedor_month 
        WHERE jefe IS NOT NULL AND jefe != '' 
        ORDER BY zona, jefe
    """).fetchall()
    
    vendedores = conn.execute("""
        SELECT DISTINCT zona, jefe, cod_vendedor, nom_vendedor 
        FROM fact_avance_cliente_vendedor_month 
        WHERE nom_vendedor IS NOT NULL 
        ORDER BY zona, jefe, nom_vendedor
    """).fetchall()
    
    conn.close()
    
    return jsonify({
        'zonas': [dict(r) for r in zonas],
        'jefes': [dict(r) for r in jefes],
        'vendedores': [dict(r) for r in vendedores]
    })


@app.route('/api/dashboard/meses-disponibles')
def api_dashboard_meses():
    """Return the last N billing months available for quick-filter buttons."""
    cod_vendedor = request.args.get('vendedor', '')
    jefe         = request.args.get('jefe', '')
    zona         = request.args.get('zona', '')

    conn = get_db()
    # Use plain column names (no table prefix); the JOIN query uses av. prefix explicitly
    if cod_vendedor:
        av_where = "av.cod_vendedor = ?"
        p = [cod_vendedor]
    elif jefe:
        av_where = "av.jefe = ?"
        p = [jefe]
    else:
        av_where = "av.zona = ?"
        p = [zona]

    # Active avance month
    avance_ym = (conn.execute(
        "SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month"
    ).fetchone()[0] or '')

    # Historical months from fact_cliente_historico scoped to this filter
    hist_rows = conn.execute(f"""
        SELECT DISTINCT h.year_month
        FROM fact_cliente_historico h
        JOIN fact_avance_cliente_vendedor_month av
          ON h.cod_cliente = av.cod_cliente AND av.year_month = ?
        WHERE {av_where}
        ORDER BY h.year_month DESC
        LIMIT 5
    """, [avance_ym] + p).fetchall()

    mes_names = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic']

    meses = []
    # Active month first
    if avance_ym:
        yp, mp = avance_ym.split('-')
        meses.append({'ym': avance_ym, 'label': mes_names[int(mp)-1]+' '+yp, 'is_active': True})

    for r in hist_rows:
        ym2 = r['year_month']
        if ym2 == avance_ym:
            continue
        yp2, mp2 = ym2.split('-')
        meses.append({'ym': ym2, 'label': mes_names[int(mp2)-1]+' '+yp2, 'is_active': False})

    conn.close()
    return jsonify({'meses': meses[:4]})  # active + up to 3 historical


@app.route('/api/dashboard')
def api_dashboard():
    """Return KPIs and chart data for a vendedor, jefe, or zona.
    Optional ?month=YYYY-MM returns historical data from fact_cliente_historico.
    """
    cod_vendedor = request.args.get('vendedor', '')
    jefe = request.args.get('jefe', '')
    zona = request.args.get('zona', '')
    req_month = request.args.get('month', '')  # optional historical month override
    
    if not any([cod_vendedor, jefe, zona]):
        return jsonify({'error': 'vendedor, jefe, or zona required'}), 400
    
    conn = get_db()
    
    # Build query based on filter
    where_clause = ""
    params = []
    
    if cod_vendedor:
        # Use vendor code directly for matching
        where_clause = "cod_vendedor = ?"
        params = [cod_vendedor]
        
        # Get vendor name for display
        name_row = conn.execute("""
            SELECT nom_vendedor FROM fact_avance_cliente_vendedor_month 
            WHERE cod_vendedor = ? LIMIT 1
        """, (cod_vendedor,)).fetchone()
        
        entity_name = name_row['nom_vendedor'] if name_row else cod_vendedor
        entity_type = "Vendedor"
    elif jefe:
        where_clause = "jefe = ?"
        params = [jefe]
        entity_name = jefe
        entity_type = "Jefe"
    else:
        where_clause = "zona = ?"
        params = [zona]
        entity_name = zona
        entity_type = "Zona"

    # ── Historical month override ────────────────────────────────────────
    # When ?month=YYYY-MM points to a past month (not in fact_avance), we
    # serve data from fact_cliente_historico + fact_facturacion instead.
    avance_ym_row = conn.execute(
        "SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month"
    ).fetchone()
    avance_ym = avance_ym_row[0] if avance_ym_row else ''

    if req_month and req_month != avance_ym:
        # Historical month view
        mes_names = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic']
        yp, mp = req_month.split('-')
        month_label = mes_names[int(mp)-1] + ' ' + yp

        # Resolve vendor codes for this filter
        vc_rows = conn.execute(f"""
            SELECT DISTINCT cod_vendedor FROM fact_avance_cliente_vendedor_month
            WHERE {where_clause} AND year_month = ?
        """, params + [avance_ym]).fetchall()
        vc = [r['cod_vendedor'] for r in vc_rows]

        # av_where: same filter but with explicit av. table prefix to avoid ambiguity
        av_where = where_clause.replace('cod_vendedor', 'av.cod_vendedor') \
                               .replace('jefe', 'av.jefe') \
                               .replace('zona', 'av.zona')

        # Summary from historico
        hist_summary = conn.execute(f"""
            SELECT SUM(h.kg_vendidos) as kg_total,
                   COUNT(DISTINCT CASE WHEN h.kg_vendidos > 0 THEN h.cod_cliente END) as compradores,
                   COUNT(DISTINCT h.cod_cliente) as total_clientes
            FROM fact_cliente_historico h
            JOIN fact_avance_cliente_vendedor_month av
              ON h.cod_cliente = av.cod_cliente AND av.year_month = ?
            WHERE {av_where} AND h.year_month = ?
        """, [avance_ym] + params + [req_month]).fetchone()

        kg_total = hist_summary['kg_total'] or 0

        # Current avance objective as reference baseline
        obj_row = conn.execute(f"""
            SELECT SUM(objetivo) as obj FROM fact_avance_cliente_vendedor_month
            WHERE {where_clause} AND year_month = ?
        """, params + [avance_ym]).fetchone()
        obj_kg = obj_row['obj'] or 0
        pct_hist = round(kg_total / obj_kg * 100, 1) if obj_kg else 0

        # Client list for that month (use vendor codes to avoid ambiguity)
        if vc:
            ph2 = ','.join(['?']*len(vc))
            cli_rows = conn.execute(f"""
                SELECT h.cod_cliente,
                       av.nom_cliente,
                       h.kg_vendidos   as facturacion,
                       av.objetivo,
                       av.pendiente,
                       av.frecuencia,
                       av.nom_vendedor,
                       s.tier
                FROM fact_cliente_historico h
                JOIN fact_avance_cliente_vendedor_month av
                  ON h.cod_cliente = av.cod_cliente AND av.year_month = ?
                LEFT JOIN fact_client_segmentation s
                  ON h.cod_cliente = s.cod_cliente AND s.year_month = ?
                WHERE h.cod_vendedor IN ({ph2}) AND h.year_month = ?
                ORDER BY h.kg_vendidos DESC LIMIT 100
            """, [avance_ym, avance_ym] + vc + [req_month]).fetchall()
        else:
            cli_rows = []

        clientes_hist = []
        for r in cli_rows:
            d = dict(r)
            d['facturacion_pesos'] = 0
            d['trend_pct'] = 0
            d['kg_prev_month'] = None
            clientes_hist.append(d)

        # Monthly evolution — last 7 historical months + always include the active avance month
        evol_rows = conn.execute(f"""
            SELECT h.year_month, SUM(h.kg_vendidos) as kg
            FROM fact_cliente_historico h
            JOIN fact_avance_cliente_vendedor_month av
              ON h.cod_cliente = av.cod_cliente AND av.year_month = ?
            WHERE {av_where}
            GROUP BY h.year_month
            ORDER BY h.year_month DESC LIMIT 7
        """, [avance_ym] + params).fetchall()
        avance_kg_row = conn.execute(f"""
            SELECT SUM(venta_actual) as kg FROM fact_avance_cliente_vendedor_month
            WHERE {where_clause} AND year_month = ?
        """, params + [avance_ym]).fetchone()
        avance_kg = round(avance_kg_row['kg'] or 0, 0) if avance_kg_row else 0
        evol = [{'ym': r['year_month'], 'kg': round(r['kg'] or 0, 0),
                 'is_active': r['year_month'] == avance_ym} for r in reversed(evol_rows)]
        # Ensure active month is present
        if not any(e['ym'] == avance_ym for e in evol):
            evol.append({'ym': avance_ym, 'kg': avance_kg, 'is_active': True})
            evol.sort(key=lambda x: x['ym'])
        # Mark selected month
        for e in evol:
            e['is_selected'] = (e['ym'] == req_month)

        conn.close()
        return jsonify({
            'is_historical': True,
            'month': req_month,
            'month_label': month_label,
            'vendedor': {
                'nombre': entity_name,
                'zona': '', 'jefe': '',
                'total_clientes': hist_summary['total_clientes'] or 0,
                'type': entity_type
            },
            'kpis': {
                'facturacion': round(kg_total, 0),
                'pendiente': None,
                'objetivo': round(obj_kg, 0),
                'cumplimiento_pct': pct_hist,
                'compradores': hist_summary['compradores'] or 0,
            },
            'chart': None,
            'evolucion': evol,
            'clientes': clientes_hist,
        })

    # Get summary
    summary = conn.execute(f"""
        SELECT 
            MAX(nom_vendedor) as nom_vendedor,
            MAX(zona) as zona,
            MAX(jefe) as jefe,
            SUM(venta_actual) as facturacion,
            SUM(pendiente) as pendiente,
            SUM(objetivo) as objetivo,
            COUNT(DISTINCT cod_cliente) as total_clientes
        FROM fact_avance_cliente_vendedor_month
        WHERE {where_clause} AND year_month = (
            SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month
        )
    """, params).fetchone()
    
    if not summary or not summary['facturacion']:
        conn.close()
        return jsonify({'error': 'No data found for selection'}), 404
        
    # Get client list (top 100 to avoid huge payloads for zones)
    # Sort by total purchase amount (facturacion + pendiente) DESC
    clientes = conn.execute(f"""
        SELECT 
            av.cod_cliente,
            av.nom_cliente,
            av.venta_actual as facturacion,
            av.pendiente,
            av.objetivo,
            av.frecuencia,
            av.nom_vendedor,
            s.tier
        FROM fact_avance_cliente_vendedor_month av
        LEFT JOIN fact_client_segmentation s ON av.cod_cliente = s.cod_cliente AND av.year_month = s.year_month
        WHERE av.{where_clause} AND av.year_month = (
            SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month
        )
        ORDER BY (av.venta_actual + COALESCE(av.pendiente, 0)) DESC
        LIMIT 100
    """, params).fetchall()

    
    # Get list of vendors for current filter
    vendor_codes_query = f"""
        SELECT DISTINCT cod_vendedor FROM fact_avance_cliente_vendedor_month 
        WHERE {where_clause} AND year_month = (SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month)
    """
    vendor_codes_rows = conn.execute(vendor_codes_query, params).fetchall()
    vendor_codes = [r['cod_vendedor'] for r in vendor_codes_rows]
    
    # 3. Get Sales in Pesos per Client (Optimization: heavy query logic)
    sales_pesos_map = {}
    if vendor_codes:
        ph = ','.join(['?'] * len(vendor_codes))
        sales_q = f"""
            SELECT cod_cliente, SUM(importe) as total_pesos
            FROM fact_facturacion
            WHERE cod_vendedor IN ({ph}) 
              AND year_month = (SELECT MAX(year_month) FROM fact_facturacion)
            GROUP BY cod_cliente
        """
        sales_rows = conn.execute(sales_q, vendor_codes).fetchall()
        sales_pesos_map = {r['cod_cliente']: r['total_pesos'] for r in sales_rows}

    # 4. Get Client List (Top 100)
    # Include monetary objectives and Tier
    # Previous month for trend
    ym_row2 = conn.execute(
        "SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month"
    ).fetchone()
    cur_ym2 = ym_row2[0] if ym_row2 else datetime.now().strftime('%Y-%m')
    y2, m2 = map(int, cur_ym2.split('-'))
    prev_dt2 = datetime(y2, m2, 1) - timedelta(days=1)
    prev_ym2 = prev_dt2.strftime('%Y-%m')

    clientes_rows = conn.execute(f"""
        SELECT
            av.cod_cliente,
            av.nom_cliente,
            av.venta_actual as facturacion,
            av.pendiente,
            av.objetivo,
            av.objetivo_pesos,
            av.frecuencia,
            av.nom_vendedor,
            s.tier,
            h_prev.kg_vendidos as kg_prev_month
        FROM fact_avance_cliente_vendedor_month av
        LEFT JOIN fact_client_segmentation s
            ON av.cod_cliente = s.cod_cliente AND av.year_month = s.year_month
        LEFT JOIN fact_cliente_historico h_prev
            ON av.cod_cliente = h_prev.cod_cliente AND h_prev.year_month = ?
        WHERE av.{where_clause} AND av.year_month = (
            SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month
        )
        ORDER BY COALESCE(av.objetivo, 0) DESC
        LIMIT 100
    """, [prev_ym2] + params).fetchall()


    # Calculate Average Price per KG from Vendor Objectives (for missing client $ goals)
    avg_price_per_kg = 0
    if cod_vendedor:
        obj_row = conn.execute("""
            SELECT objetivo_pesos, objetivo_kg 
            FROM vendedor_objetivos 
            WHERE cod_vendedor = ? 
            ORDER BY year_month DESC LIMIT 1
        """, (cod_vendedor,)).fetchone()
        
        if obj_row and obj_row['objetivo_kg'] > 0:
            avg_price_per_kg = obj_row['objetivo_pesos'] / obj_row['objetivo_kg']
    
    clientes_list = []
    for r in clientes_rows:
        d = dict(r)
        d['facturacion_pesos'] = sales_pesos_map.get(r['cod_cliente'], 0)

        # Dynamic Objective Calculation
        if not d['objetivo_pesos'] and d['objetivo'] and avg_price_per_kg > 0:
            d['objetivo_pesos'] = d['objetivo'] * avg_price_per_kg

        # Trend vs previous month
        prev_kg  = d.pop('kg_prev_month', None) or 0
        fact_kg_ = d['facturacion'] or 0
        if prev_kg and prev_kg > 0:
            d['trend_pct'] = round((fact_kg_ - prev_kg) / prev_kg * 100, 1)
        elif fact_kg_ > 0 and prev_kg == 0:
            d['trend_pct'] = None  # new buyer, no prior data
        else:
            d['trend_pct'] = 0

        clientes_list.append(d)
    
    
    
    daily_sales = []
    composition = []
    
    if vendor_codes:
        placeholders = ','.join(['?'] * len(vendor_codes))
        
        # 1. Daily Sales for Burn Chart (Volume in KG)
        daily_query = f"""
            SELECT 
                strftime('%d', fecha_emision) as dia,
                COALESCE(SUM(cantidad), 0) as venta
            FROM fact_facturacion
            WHERE cod_vendedor IN ({placeholders}) 
              AND year_month = (SELECT MAX(year_month) FROM fact_facturacion)
            GROUP BY dia
            ORDER BY dia
        """
        daily_rows = conn.execute(daily_query, vendor_codes).fetchall()
        
        # Accumulate sales
        acum = 0
        for r in daily_rows:
            v = r['venta'] or 0
            acum += v
            daily_sales.append({
                'dia': int(r['dia']),
                'venta_dia': v,
                'acumulado': acum
            })
            
        # 2. Composition (Premium/Commodity + Family)
        # Use dim_product_classification joined
        comp_query = f"""
            SELECT 
                COALESCE(p.subcategoria, 'OTROS') as tipo,
                COALESCE(p.categoria, 'SIN CATEGORIA') as familia,
                SUM(f.importe) as valor
            FROM fact_facturacion f
            LEFT JOIN dim_product_classification p ON f.cod_producto = p.cod_producto
            WHERE f.cod_vendedor IN ({placeholders})
              AND f.year_month = (SELECT MAX(year_month) FROM fact_facturacion)
            GROUP BY tipo, familia
            ORDER BY valor DESC
        """
        comp_rows = conn.execute(comp_query, vendor_codes).fetchall()
        composition = [dict(r) for r in comp_rows]

        # 3. Process Chart Data
        # Aligned with Cierre de Mes: use business days (lun-vie) for ideal and projection
        ym_res = conn.execute("SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month").fetchone()
        ym = ym_res[0] if ym_res else datetime.now().strftime('%Y-%m')
        year, month = map(int, ym.split('-'))
        days_in_month = calendar.monthrange(year, month)[1]

        # Total business days in month
        total_bd = sum(1 for d in range(1, days_in_month + 1)
                       if datetime(year, month, d).weekday() < 5)

        dates = list(range(1, days_in_month + 1))
        dates_s = [str(d) for d in dates]

        # Ideal Line: cumulative objetivo by business days (same logic as Cierre de Mes)
        total_objetivo = summary['objetivo'] or 0
        ideal = []
        bd_so_far = 0
        for d in dates:
            if datetime(year, month, d).weekday() < 5:
                bd_so_far += 1
            ideal.append(round(total_objetivo * bd_so_far / total_bd, 0) if total_bd else 0)

        # Actual Line (Cumulative)
        daily_map = {x['dia']: x['acumulado'] for x in daily_sales}
        actual = []
        last_val = 0
        today = datetime.now().day
        is_current_month = (ym == datetime.now().strftime('%Y-%m'))
        max_day = today if is_current_month else days_in_month
        max_day = min(max_day, days_in_month)

        for d in dates:
            if d <= max_day:
                val = daily_map.get(d)
                if val is not None:
                    last_val = val
                actual.append(round(last_val, 0))
            else:
                break

        # Projection: blend linear + ratio-based (same as Cierre de Mes)
        projection = [None] * len(actual)
        if actual and total_objetivo > 0 and max_day > 0 and max_day < days_in_month:
            current_total = actual[-1]
            bd_elapsed = sum(1 for d in range(1, max_day + 1)
                             if datetime(year, month, d).weekday() < 5)
            remaining_bd = sum(1 for d in range(max_day + 1, days_in_month + 1)
                               if datetime(year, month, d).weekday() < 5)
            avg_daily = current_total / bd_elapsed if bd_elapsed > 0 else 0
            proj_linear = current_total + avg_daily * remaining_bd
            # Ratio-based: historical cumulative-at-day-N / final (align with Cierre de Mes)
            proj_ratio = None
            hist_ratios = conn.execute(f"""
                SELECT year_month, SUM(kg) as total_mes,
                       SUM(CASE WHEN dia <= ? THEN kg ELSE 0 END) as acum_n
                FROM (
                    SELECT year_month, CAST(strftime('%d', fecha_emision) AS INTEGER) as dia, SUM(cantidad) as kg
                    FROM fact_facturacion
                    WHERE cod_vendedor IN ({placeholders}) AND year_month != ?
                    GROUP BY year_month, dia
                ) x
                GROUP BY year_month
                HAVING total_mes > 0
            """, [max_day] + vendor_codes + [ym]).fetchall()
            if hist_ratios:
                import statistics as _st
                ratios = [r['acum_n'] / r['total_mes'] for r in hist_ratios if r['acum_n'] and r['total_mes']]
                if ratios and _st.mean(ratios) > 0.02:
                    proj_ratio = current_total / _st.mean(ratios)
            final_proj = (0.5 * proj_linear + 0.5 * proj_ratio) if proj_ratio else proj_linear
            remaining_kg = final_proj - current_total
            remaining_cal = days_in_month - max_day
            for i in range(1, remaining_cal + 1):
                projection.append(round(current_total + remaining_kg * i / remaining_cal, 0))

        # Consolidate Chart Data
        total_pendiente = summary['pendiente'] or 0
        actual_plus_pend = [round(v + total_pendiente, 0) for v in actual]
        
        chart_data = {
            'dates': dates_s,
            'ideal': ideal,
            'actual': actual,
            'real_plus_pend': actual_plus_pend,
            'projection': projection,
            'objetivo': round(total_objetivo, 0),
            'facturado': round(summary['facturacion'] or 0, 0),
            'pendiente': round(total_pendiente, 0),
            'composition': composition
        }

    total_facturado = summary['facturacion'] or 0
    total_pendiente = summary['pendiente'] or 0
    # total_objetivo already defined above
    pct = round((total_facturado + total_pendiente) / total_objetivo * 100, 1) if total_objetivo else 0
    
    # Determine display name
    display_name = summary['nom_vendedor'] if cod_vendedor else (jefe if jefe else zona)

    # Monthly evolution (last 6 months) for the clickable evol chart
    # Use av. prefix on where_clause columns to avoid ambiguous column names
    av_where_main = where_clause.replace('cod_vendedor', 'av.cod_vendedor') \
                                .replace('jefe', 'av.jefe') \
                                .replace('zona', 'av.zona')
    evol_rows = conn.execute(f"""
        SELECT h.year_month, SUM(h.kg_vendidos) as kg
        FROM fact_cliente_historico h
        JOIN fact_avance_cliente_vendedor_month av
          ON h.cod_cliente = av.cod_cliente AND av.year_month = ?
        WHERE {av_where_main}
        GROUP BY h.year_month
        ORDER BY h.year_month DESC LIMIT 5
    """, [avance_ym] + params).fetchall()
    # Add the active month (avance)
    evol_active = {'ym': avance_ym, 'kg': round(total_facturado, 0), 'is_active': True, 'is_selected': True}
    evol = [evol_active] + [{'ym': r['year_month'], 'kg': round(r['kg'] or 0, 0),
                              'is_active': False, 'is_selected': False}
                             for r in evol_rows if r['year_month'] != avance_ym]
    evol.sort(key=lambda x: x['ym'])

    conn.close()

    return jsonify({
        'is_historical': False,
        'month': avance_ym,
        'vendedor': {
            'nombre': display_name,
            'zona': summary['zona'] if not zona else zona,
            'jefe': summary['jefe'] if not jefe else jefe,
            'total_clientes': summary['total_clientes'],
            'type': entity_type
        },
        'kpis': {
            'facturacion': round(total_facturado, 2),
            'pendiente': round(total_pendiente, 2),
            'objetivo': round(total_objetivo, 2),
            'cumplimiento_pct': pct
        },
        'chart': chart_data,
        'evolucion': evol,
        'clientes': clientes_list
    })

@app.route('/planning')
def planning():
    return render_template('planning.html')

@app.route('/api/planning')
def api_planning():
    vendedor = request.args.get('vendedor')
    date_str = request.args.get('date') # YYYY-MM-DD
    
    if not date_str:
        date_str = datetime.now().strftime('%Y-%m-%d')
        
    target_date = datetime.strptime(date_str, '%Y-%m-%d')
    weekday = target_date.weekday() # 0=Mon, 6=Sun
    
    # Mapping Sales Day -> Delivery Days (Frecuencia)
    # Rule: Sell 24/48h before delivery
    # Mon (0) -> Tue, Wed
    # Tue (1) -> Wed, Thu
    # Wed (2) -> Thu, Fri
    # Thu (3) -> Fri, Mon
    # Fri (4) -> Mon, Tue
    # Sat/Sun -> None
    
    mapping = {
        0: ['MARTES', 'MIERCOLES'],
        1: ['MIERCOLES', 'JUEVES'],
        2: ['JUEVES', 'VIERNES'],
        3: ['VIERNES', 'LUNES'],
        4: ['LUNES', 'MARTES'],
        5: [],
        6: []
    }
    
    target_frecuencias = mapping.get(weekday, [])
    
    conn = get_db()
    
    # Get clients with matching frequency
    # We need to filter by vendedor first
    # Reuse vendor mapping logic if needed, but for simplicity let's assume direct code or mapped code
    # Actually we need to match the 'clientes' logic from api_dashboard
    
    # 1. Resolve vendor code / filters
    where_parts = []
    params = []
    
    # ... logic similar to api_dashboard to filter clients ...
    # But filtering by PRE-CALCULATED client list in Avance might be better?
    # Or joining with dim_clients directly?
    # dim_clients has 'frecuencia'.
    
    # Let's simplify: Query dim_clientes joined with Avance for the vendor
    # We need 'cod_cliente' from Avance matching filters
    
    # ... (Copying filter logic) ...
    jefe = request.args.get('jefe')
    zona = request.args.get('zona')
    
    base_query = """
        SELECT a.cod_cliente, a.nom_cliente, a.frecuencia, a.objetivo, a.venta_actual as facturacion, a.pendiente
        FROM fact_avance_cliente_vendedor_month a
        WHERE 1=1
    """
    
    if vendedor:
        base_query += " AND a.cod_vendedor = ?"
        params.append(vendedor)
    elif jefe:
        base_query += " AND a.jefe = ?"
        params.append(jefe)
    elif zona:
        base_query += " AND a.zona = ?"
        params.append(zona)
        
    base_query += " AND a.year_month = (SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month)"
    
    all_clients = conn.execute(base_query, params).fetchall()
    
    # Filter by frequency in Python to avoid complex SQL IN clause with strings
    planning_clients = []
    for c in all_clients:
        freq = (c['frecuencia'] or '').upper()
        # Check if any target freq is in client freq (in case of multiple)
        # Assuming freq is single word for now based on previous check
        if freq in target_frecuencias:
            planning_clients.append(dict(c))
            
    # 2. Global KPIs for Catch-up calculation
    # Robust way to build sum query: replace SELECT part
    from_index = base_query.find("FROM")
    kpi_query = "SELECT SUM(a.objetivo) as obj, SUM(a.venta_actual) as fact " + base_query[from_index:]
    
    kpi_row = conn.execute(kpi_query, params).fetchone()
    
    total_obj = kpi_row['obj'] or 0
    total_fact = kpi_row['fact'] or 0
    gap = total_obj - total_fact
    
    # KPIs for the day
    total_clients = len(planning_clients)
    total_objective = sum([c['objetivo'] or 0 for c in planning_clients])
    
    # 3. Historical Average (User Request: "Potencial Basado en Historico")
    client_codes = [c['cod_cliente'] for c in planning_clients]
    if client_codes:
        placeholders = ','.join(['?'] * len(client_codes))
        hist_query = f"""
            SELECT cod_cliente, AVG(kg_vendidos) as avg_kg 
            FROM fact_cliente_historico 
            WHERE cod_cliente IN ({placeholders}) 
            GROUP BY cod_cliente
        """
        try:
            hist_rows = conn.execute(hist_query, client_codes).fetchall()
            hist_map = {r['cod_cliente']: (r['avg_kg'] or 0) for r in hist_rows}
        except:
            hist_map = {}
    else:
        hist_map = {}
        
    for c in planning_clients:
        c['historico'] = hist_map.get(c['cod_cliente'], 0)

    # Calculate Total Potential based on History
    total_potential_hist = sum([c['historico'] for c in planning_clients])

    # Días hábiles: Lun–Vie
    is_business_day = weekday < 5
    import calendar
    last_day = calendar.monthrange(target_date.year, target_date.month)[1]
    month_end = target_date.replace(day=last_day)
    total_bd = sum(1 for d in range(1, last_day + 1)
                   if datetime(target_date.year, target_date.month, d).weekday() < 5)
    days_remaining = 0
    current = target_date
    while current <= month_end:
        if current.weekday() < 5:
            days_remaining += 1
        current += timedelta(days=1)

    # Proyección histórica por día de semana: kg vendidos este día (Lun, Mar, etc.) en meses anteriores
    proyeccion_historico_dia_kg = None
    vc_rows = conn.execute(f"""
        SELECT DISTINCT cod_vendedor FROM fact_avance_cliente_vendedor_month
        WHERE {"cod_vendedor = ?" if vendedor else "jefe = ?" if jefe else "zona = ?"}
    """, ([vendedor] if vendedor else [jefe] if jefe else [zona])).fetchall()
    vendor_codes = [r['cod_vendedor'] for r in vc_rows]
    if vendor_codes:
        ph = ','.join(['?'] * len(vendor_codes))
        # SQLite strftime('%w') = 0 Sun, 1 Mon, ... 6 Sat. weekday() = 0 Mon, 6 Sun
        sqlite_dow = (weekday + 1) % 7  # Mon=1, Tue=2, ..., Sun=0
        hist_dia = conn.execute(f"""
            SELECT SUM(cantidad) as kg, COUNT(DISTINCT year_month) as meses
            FROM fact_facturacion
            WHERE cod_vendedor IN ({ph}) AND year_month < ?
              AND CAST(strftime('%w', fecha_emision) AS INTEGER) = ?
        """, vendor_codes + [target_date.strftime('%Y-%m'), sqlite_dow]).fetchone()
        if hist_dia and hist_dia['meses'] and hist_dia['meses'] > 0:
            proyeccion_historico_dia_kg = round((hist_dia['kg'] or 0) / hist_dia['meses'], 0)

    conn.close()

    daily_needed = (gap / days_remaining) if (days_remaining > 0 and is_business_day) else None
    daily_avg = total_obj / total_bd if total_bd else total_obj / 20

    dia_nombres = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
    weekday_name_es = dia_nombres[weekday] if weekday < 7 else target_date.strftime('%A')

    return jsonify({
        'date': date_str,
        'weekday_name': weekday_name_es,
        'target_frequencies': target_frecuencias,
        'is_business_day': is_business_day,
        'clients': planning_clients,
        'stats': {
            'count': total_clients,
            'clients_objective_sum': total_objective,
            'clients_historical_sum': total_potential_hist,
            'global_objective': total_obj,
            'gap': gap,
            'days_remaining': days_remaining,
            'total_bd': total_bd,
            'daily_needed': daily_needed,
            'daily_average': daily_avg,
            'proyeccion_historico_dia_kg': proyeccion_historico_dia_kg,
        }
    })


@app.route('/api/cliente/<cod_cliente>', methods=['GET', 'PUT'])
@login_required
def api_cliente(cod_cliente):
    """Return or update client detail."""
    conn = get_db()

    if request.method == 'PUT':
        data = request.json
        # Update dim_clients editable fields (master data enrichment)
        conn.execute("""
            UPDATE dim_clients SET
                contacto   = ?,
                telefono   = ?,
                correo     = ?,
                direccion  = ?,
                ciudad     = ?,
                provincia  = ?,
                plazo      = ?,
                canal      = ?,
                activo     = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE cliente_id = ?
        """, (data.get('contacto'), data.get('telefono'), data.get('correo'),
              data.get('direccion'), data.get('ciudad'), data.get('provincia'),
              data.get('plazo'), data.get('canal'), data.get('activo'), cod_cliente))
        # Upsert CRM enrichment (nivel, estado, frecuencia, notas)
        conn.execute("""
            INSERT INTO crm_accounts (cod_cliente, nivel, estado, contacto_nombre,
                contacto_telefono, contacto_email, frecuencia_visita, notas_cuenta, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(cod_cliente) DO UPDATE SET
                nivel             = excluded.nivel,
                estado            = excluded.estado,
                contacto_nombre   = excluded.contacto_nombre,
                contacto_telefono = excluded.contacto_telefono,
                contacto_email    = excluded.contacto_email,
                frecuencia_visita = excluded.frecuencia_visita,
                notas_cuenta      = excluded.notas_cuenta,
                updated_at        = CURRENT_TIMESTAMP
        """, (cod_cliente, data.get('nivel'), data.get('estado'),
              data.get('crm_contacto_nombre'), data.get('crm_contacto_telefono'),
              data.get('correo'), data.get('crm_frecuencia_visita'), data.get('crm_notas')))
        conn.commit()
        conn.close()
        return jsonify({'status': 'success'})

    meses = int(request.args.get('meses', 6))
    
    conn = get_db()
    
    # Get client info with Tier and contact data
    cliente = conn.execute("""
        SELECT 
            av.cod_cliente, 
            av.nom_cliente, 
            av.frecuencia,
            s.tier,
            s.score,
            s.vol_score,
            s.mix_score,
            s.loyalty_score,
            dc.ciudad,
            dc.provincia,
            dc.direccion,
            dc.telefono,
            dc.correo,
            dc.contacto,
            dc.plazo,
            dc.lat,
            dc.lon
        FROM fact_avance_cliente_vendedor_month av
        LEFT JOIN fact_client_segmentation s ON av.cod_cliente = s.cod_cliente AND av.year_month = s.year_month
        LEFT JOIN dim_clients dc ON av.cod_cliente = dc.cliente_id
        WHERE av.cod_cliente = ?
          AND av.year_month = (SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month)
    """, (cod_cliente,)).fetchone()
    
    if not cliente:
        conn.close()
        return jsonify({'error': 'cliente not found'}), 404

    
    # Get the current active month from fact_facturacion
    current_ym_row = conn.execute("SELECT MAX(year_month) FROM fact_facturacion").fetchone()
    current_ym = current_ym_row[0] if current_ym_row else None
    
    # Historia chart: use fact_cliente_historico (net, from avance Excel) for closed months.
    # These already reflect NC deductions. Add the current billing month from fact_facturacion.
    # NOTE: legacy TXT months include NCs (negative rows) so their net sum matches the Excel.
    #       Minerva-format months (Feb26+) may be gross (no NCs in file) — we label them clearly.
    historia_rows = conn.execute("""
        SELECT year_month,
               kg_vendidos as total_kg,
               NULL as total_importe,
               NULL as n_productos,
               0 as has_nc_risk
        FROM fact_cliente_historico
        WHERE cod_cliente = ?
        ORDER BY year_month ASC
    """, (cod_cliente,)).fetchall()
    historia = [dict(h) for h in historia_rows]

    # Add current month from fact_facturacion (gross) if not in history
    if current_ym and not any(h['year_month'] == current_ym for h in historia):
        cur_row = conn.execute("""
            SELECT SUM(cantidad) as total_kg, SUM(importe) as total_importe,
                   COUNT(DISTINCT cod_producto) as n_productos,
                   SUM(CASE WHEN cantidad < 0 THEN cantidad ELSE 0 END) as kg_nc
            FROM fact_facturacion WHERE cod_cliente = ? AND year_month = ?
        """, (cod_cliente, current_ym)).fetchone()
        if cur_row and cur_row['total_kg']:
            nc_risk = 1 if (cur_row['kg_nc'] or 0) == 0 else 0  # 1 = no NCs found (may be gross)
            historia.append({
                'year_month': current_ym,
                'total_kg': cur_row['total_kg'],
                'total_importe': cur_row['total_importe'],
                'n_productos': cur_row['n_productos'],
                'has_nc_risk': nc_risk
            })
            historia.sort(key=lambda x: x['year_month'])

    # Get the last N available year_months from fact_facturacion for this client's data
    all_months = conn.execute("""
        SELECT DISTINCT year_month FROM fact_facturacion
        ORDER BY year_month DESC
        LIMIT ?
    """, (meses,)).fetchall()

    
    month_cutoff = all_months[-1]['year_month'] if all_months else '2000-01'
    
    # Get product category breakdown for this client (last N available months)
    categorias = conn.execute("""
        SELECT 
            COALESCE(p.categoria, 'SIN CATEGORIA') as categoria,
            SUM(f.cantidad) as total_kg,
            SUM(f.importe) as total_importe,
            COUNT(DISTINCT f.cod_producto) as productos
        FROM fact_facturacion f
        LEFT JOIN dim_product_classification p ON f.cod_producto = p.cod_producto
        WHERE f.cod_cliente = ?
          AND f.year_month >= ?
        GROUP BY p.categoria
        ORDER BY total_kg DESC
    """, (cod_cliente, month_cutoff)).fetchall()
    
    # Get individual product purchases (top 15, last N available months)
    productos = conn.execute("""
        SELECT 
            f.cod_producto,
            COALESCE(p.descripcion, 'PRODUCTO SIN NOMBRE') as nombre_producto,
            COALESCE(p.categoria, 'SIN CATEGORIA') as categoria,
            SUM(f.cantidad) as total_kg,
            SUM(f.importe) as total_importe,
            COUNT(*) as veces_comprado
        FROM fact_facturacion f
        LEFT JOIN dim_product_classification p ON f.cod_producto = p.cod_producto
        WHERE f.cod_cliente = ?
          AND f.year_month >= ?
        GROUP BY f.cod_producto, p.descripcion, p.categoria
        ORDER BY total_kg DESC
        LIMIT 15
    """, (cod_cliente, month_cutoff)).fetchall()
    
    # Get current month objective/progress
    # Filter by vendor if provided to avoid picking up other vendor's data for shared clients
    vendedor_arg = request.args.get('vendedor')
    
    avance_query = """
        SELECT 
            venta_actual as facturacion,
            pendiente,
            objetivo,
            objetivo_pesos,
            objetivo_premium_pesos,
            nom_vendedor,
            cod_vendedor,
            zona
        FROM fact_avance_cliente_vendedor_month
        WHERE cod_cliente = ? 
          AND year_month = (SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month)
    """
    params = [cod_cliente]
    
    if vendedor_arg and vendedor_arg != 'undefined':
        avance_query += " AND cod_vendedor = ?"
        params.append(vendedor_arg)
        
    avance = conn.execute(avance_query, params).fetchone()
    
    # Calculate current month sales ($ and Premium $ and Rebozados KG)
    current_sales_query = """
        SELECT 
            SUM(f.importe) as venta_actual_pesos,
            SUM(CASE WHEN p.subcategoria = 'PREMIUM' THEN f.importe ELSE 0 END) as venta_premium_pesos,
            SUM(CASE WHEN p.categoria = 'REBOZADOS' THEN f.cantidad ELSE 0 END) as rebozados_kg
        FROM fact_facturacion f
        LEFT JOIN dim_product_classification p ON f.cod_producto = p.cod_producto
        WHERE f.cod_cliente = ? 
          AND f.year_month = (SELECT MAX(year_month) FROM fact_facturacion)
    """
    sales_params = [cod_cliente]
    if vendedor_arg and vendedor_arg != 'undefined':
        current_sales_query += " AND f.cod_vendedor = ?"
        sales_params.append(vendedor_arg)
        
    current_sales = conn.execute(current_sales_query, sales_params).fetchone()

    # When fact_facturacion has no data for the current month (e.g. TXT export is
    # partial and this client hasn't been invoiced yet), estimate pesos using the
    # client's historical average price per KG from recent months.
    fact_pesos = current_sales['venta_actual_pesos'] if current_sales else None
    if not fact_pesos and avance:
        venta_kg = avance['facturacion'] or 0   # venta_actual KG from avance table
        if venta_kg > 0:
            price_row = conn.execute("""
                SELECT ROUND(SUM(importe) / NULLIF(SUM(cantidad), 0), 2) as avg_price
                FROM fact_facturacion
                WHERE cod_cliente = ? AND cantidad > 0 AND importe > 0
                  AND year_month >= (
                      SELECT date(MAX(year_month) || '-01', '-3 months')
                      FROM fact_facturacion
                      WHERE cod_cliente = ?
                  )
            """, (cod_cliente, cod_cliente)).fetchone()
            avg_price = price_row['avg_price'] if price_row and price_row['avg_price'] else 0
            if avg_price > 0:
                fact_pesos = round(venta_kg * avg_price, 0)

    # ── Facturas recientes ────────────────────────────────────────────────────
    # Business rule: a purchase date D proves that ALL invoices whose due date
    # (fecha_emision + plazo) fell BEFORE D have been paid — the system would
    # not allow a new order with outstanding overdue debt.
    # Therefore we only surface invoices where NO subsequent purchase has
    # occurred AFTER that invoice's due date, and that are not manually marked.
    plazo_row = conn.execute(
        "SELECT CAST(plazo AS INTEGER) as plazo_dias FROM dim_clients WHERE cliente_id = ?",
        (cod_cliente,)
    ).fetchone()
    plazo_dias = plazo_row['plazo_dias'] if plazo_row and plazo_row['plazo_dias'] else 0

    facturas_rows = conn.execute("""
        WITH inv AS (
            SELECT
                fecha_emision,
                ROUND(SUM(importe), 0)                                    AS importe_total,
                ROUND(SUM(CASE WHEN cantidad > 0 THEN cantidad ELSE 0 END), 1) AS kg_total,
                CAST(julianday('now') - julianday(MIN(fecha_emision)) AS INTEGER) AS dias_desde_emision,
                date(MIN(fecha_emision), '+' || ? || ' days')             AS fecha_vencimiento
            FROM fact_facturacion
            WHERE cod_cliente = ?
              AND fecha_emision >= date('now', '-120 days')
              AND importe != 0
            GROUP BY fecha_emision
        )
        SELECT inv.*,
               -- implicitly paid: a later purchase happened AFTER the due date
               CASE WHEN EXISTS (
                   SELECT 1 FROM fact_facturacion f2
                   WHERE f2.cod_cliente = ?
                     AND f2.fecha_emision > inv.fecha_vencimiento
               ) THEN 1 ELSE 0 END AS auto_pagada,
               -- manually marked paid
               CASE WHEN EXISTS (
                   SELECT 1 FROM fact_factura_pagada fp
                   WHERE fp.cod_cliente = ?
                     AND fp.fecha_emision = inv.fecha_emision
               ) THEN 1 ELSE 0 END AS manual_pagada
        FROM inv
        WHERE auto_pagada = 0 AND manual_pagada = 0
        ORDER BY inv.fecha_emision DESC
        LIMIT 20
    """, (plazo_dias, cod_cliente, cod_cliente, cod_cliente)).fetchall()

    facturas_recientes = []
    for fr in facturas_rows:
        dias = fr['dias_desde_emision'] or 0
        fv   = fr['fecha_vencimiento']
        vencida = plazo_dias > 0 and dias > plazo_dias
        dias_vencida = max(0, dias - plazo_dias) if vencida else 0
        facturas_recientes.append({
            'fecha_emision':      fr['fecha_emision'],
            'fecha_vencimiento':  fv,
            'importe_total':      fr['importe_total'] or 0,
            'kg_total':           fr['kg_total'] or 0,
            'dias_desde_emision': dias,
            'vencida':            vencida,
            'dias_vencida':       dias_vencida,
            'plazo_dias':         plazo_dias,
        })

    # Calculate weighted objetivo for rebozados if we have vendor info
    # When client has custom ponderacion, use that %; else use proportion from objetivo
    objetivo_rebozados_kg = 0
    ym = conn.execute("SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month").fetchone()[0]
    ponderacion_row = conn.execute(
        "SELECT ponderacion_pct FROM crm_cliente_ponderacion WHERE cod_cliente = ? AND year_month = ?",
        (cod_cliente, ym)
    ).fetchone() if ym else None
    custom_proportion = (ponderacion_row['ponderacion_pct'] / 100.0) if ponderacion_row and ponderacion_row['ponderacion_pct'] is not None else None

    if avance and vendedor_arg and vendedor_arg != 'undefined':
        vendor_obj = conn.execute("""
            SELECT objetivo_rebozados_kg, objetivo_kg, obj_hg, obj_sch
            FROM vendedor_objetivos
            WHERE cod_vendedor = ?
            ORDER BY year_month DESC LIMIT 1
        """, (vendedor_arg,)).fetchone()

        if vendor_obj and (vendor_obj['objetivo_rebozados_kg'] or 0) > 0:
            vendor_rebozados_objetivo = vendor_obj['objetivo_rebozados_kg'] or 0
            if custom_proportion is not None:
                proportion = min(custom_proportion, 1.0)
            else:
                client_kg_objetivo = avance['objetivo'] or 0
                sum_row = conn.execute("""
                    SELECT SUM(objetivo) as total_kg
                    FROM fact_avance_cliente_vendedor_month
                    WHERE cod_vendedor = ?
                      AND year_month = (SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month)
                      AND objetivo > 0
                """, (vendedor_arg,)).fetchone()
                base_kg = (sum_row['total_kg'] or 0) if sum_row else 0
                if base_kg == 0:
                    base_kg = vendor_obj['objetivo_kg'] or 0
                proportion = min(client_kg_objetivo / base_kg, 1.0) if base_kg > 0 else 0
            objetivo_rebozados_kg = vendor_rebozados_objetivo * proportion
    
    # Merge into avance dict
    avance_dict = dict(avance) if avance else {}
    if current_sales:
        # Use estimated pesos when fact_facturacion has no current-month data yet
        avance_dict['facturacion_pesos'] = fact_pesos or 0
        avance_dict['premium_pesos'] = current_sales['venta_premium_pesos'] or 0
        avance_dict['rebozados_kg'] = current_sales['rebozados_kg'] or 0
        avance_dict['facturacion_pesos_estimated'] = (fact_pesos or 0) > 0 and not (current_sales['venta_actual_pesos'] or 0)

    avance_dict['objetivo_rebozados_kg'] = round(objetivo_rebozados_kg, 2)
    if ponderacion_row and ponderacion_row['ponderacion_pct'] is not None:
        avance_dict['ponderacion_pct'] = ponderacion_row['ponderacion_pct']
    
    conn.close()
    
    return jsonify({
        'cliente': dict(cliente) if cliente else None,
        'avance': avance_dict if avance_dict else None,
        'historia': [dict(h) for h in historia],
        'categorias': [dict(c) for c in categorias],
        'productos': [dict(p) for p in productos],
        'facturas_recientes': facturas_recientes,
        'plazo_dias': plazo_dias,
    })


@app.route('/api/cliente/<cod_cliente>/facturas/<fecha_emision>/pagar', methods=['POST'])
@login_required
def api_factura_marcar_pagada(cod_cliente, fecha_emision):
    """Manually mark an invoice date as paid so it disappears from the pending list."""
    conn = get_db()
    try:
        conn.execute("""
            INSERT OR REPLACE INTO fact_factura_pagada (cod_cliente, fecha_emision, marked_by)
            VALUES (?, ?, ?)
        """, (cod_cliente, fecha_emision, session.get('user', 'unknown')))
        conn.commit()
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500
    finally:
        conn.close()


@app.route('/api/vendedor/<cod_vendedor>/facturacion')
def api_vendedor_facturacion(cod_vendedor=None):
    """Return vendedor facturacion with premium breakdown and objectives."""
    cod_vendedor = request.args.get('vendedor', cod_vendedor)
    jefe = request.args.get('jefe')
    zona = request.args.get('zona')
    
    conn = get_db()
    
    where_clause = ""
    params = []
    
    # 1. Determine scope and build query
    if cod_vendedor and cod_vendedor != 'undefined':
        # Use simple code matching since we consolidated data
        where_clause = "f.cod_vendedor = ?"
        params = [cod_vendedor]
        
        # For objectives
        obj_where = "cod_vendedor = ?"
        obj_params = [cod_vendedor]
        
    elif jefe:
        vendedores = conn.execute("""
            SELECT DISTINCT cod_vendedor FROM fact_avance_cliente_vendedor_month WHERE jefe = ?
        """, (jefe,)).fetchall()
        
        codes = [v['cod_vendedor'] for v in vendedores]
        if not codes:
            conn.close()
            return jsonify({})
            
        placeholders = ','.join(['?'] * len(codes))
        where_clause = f"f.cod_vendedor IN ({placeholders})"
        params = codes
        
        obj_where = f"cod_vendedor IN ({placeholders})"
        obj_params = codes
        
    elif zona:
         # Link facturacion to zona
        vendedores = conn.execute("""
            SELECT DISTINCT cod_vendedor FROM fact_avance_cliente_vendedor_month WHERE zona = ?
        """, (zona,)).fetchall()
        
        codes = [v['cod_vendedor'] for v in vendedores]
        if not codes:
            conn.close()
            return jsonify({})
            
        placeholders = ','.join(['?'] * len(codes))
        where_clause = f"f.cod_vendedor IN ({placeholders})"
        params = codes
        
        obj_where = f"cod_vendedor IN ({placeholders})"
        obj_params = codes

    else:
        conn.close()
        return jsonify({})

    # Get facturacion totals
    facturacion = conn.execute(f"""
        SELECT 
            SUM(f.importe) as total_pesos,
            SUM(f.cantidad) as total_kg,
            SUM(CASE WHEN p.subcategoria = 'PREMIUM' THEN f.importe ELSE 0 END) as premium_pesos,
            SUM(CASE WHEN p.categoria = 'REBOZADOS' THEN f.cantidad ELSE 0 END) as rebozados_kg
        FROM fact_facturacion f
        LEFT JOIN dim_product_classification p ON f.cod_producto = p.cod_producto
        WHERE {where_clause} AND year_month = (SELECT MAX(year_month) FROM fact_facturacion)
    """, params).fetchone()
    
    # Get objectives
    objetivos = conn.execute(f"""
        SELECT 
            SUM(objetivo_pesos) as objetivo_pesos, 
            SUM(objetivo_premium_pesos) as objetivo_premium_pesos, 
            SUM(objetivo_kg) as objetivo_kg,
            SUM(objetivo_rebozados_kg) as objetivo_rebozados_kg
        FROM vendedor_objetivos WHERE {obj_where}
        AND year_month = (SELECT MAX(year_month) FROM vendedor_objetivos)
    """, obj_params).fetchone()
    
    # Get product category breakdown
    categorias = conn.execute(f"""
        SELECT 
            COALESCE(p.categoria, 'SIN CATEGORIA') as categoria,
            p.subcategoria,
            SUM(f.cantidad) as kg,
            SUM(f.importe) as pesos,
            COUNT(DISTINCT f.cod_producto) as productos
        FROM fact_facturacion f
        LEFT JOIN dim_product_classification p ON f.cod_producto = p.cod_producto
        WHERE {where_clause} AND f.year_month = (SELECT MAX(year_month) FROM fact_facturacion)
        GROUP BY p.categoria, p.subcategoria
        ORDER BY pesos DESC
    """, params).fetchall()
    
    conn.close()
    
    return jsonify({
        'facturacion': dict(facturacion) if facturacion else None,
        'objetivos': dict(objetivos) if objetivos else None,
        'categorias': [dict(c) for c in categorias]
    })


# ==================== CRM ADM API ENDPOINTS ====================

@app.route('/api/crm/portfolio')
@login_required
def api_crm_portfolio():
    """Return the executive's account portfolio with CRM enrichment, ponderación, and last activity."""
    cod_vendedor = request.args.get('vendedor', '')
    jefe = request.args.get('jefe', '')
    zona = request.args.get('zona', '')
    conn = get_db()

    ym_row = conn.execute("SELECT MAX(year_month) as ym FROM fact_avance_cliente_vendedor_month").fetchone()
    year_month = ym_row['ym'] if ym_row else None

    where_parts = ["av.year_month = (SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month)"]
    params = []
    if cod_vendedor:
        where_parts.append("av.cod_vendedor = ?"); params.append(cod_vendedor)
    elif jefe:
        where_parts.append("av.jefe = ?"); params.append(jefe)
    elif zona:
        where_parts.append("av.zona = ?"); params.append(zona)

    where = " AND ".join(where_parts)

    # Totals for ponderación (active clients with objetivo > 0)
    total_row = conn.execute(f"""
        SELECT
            COALESCE(SUM(av.objetivo), 0) as total_objetivo,
            COALESCE(SUM(av.objetivo_pesos), 0) as total_objetivo_pesos,
            COALESCE(SUM(av.objetivo_premium_pesos), 0) as total_objetivo_premium
        FROM fact_avance_cliente_vendedor_month av
        WHERE {where} AND av.objetivo > 0
    """, params).fetchone()
    total_objetivo = total_row['total_objetivo'] or 0
    total_objetivo_pesos = total_row['total_objetivo_pesos'] or 0
    total_objetivo_premium = total_row['total_objetivo_premium'] or 0

    rows = conn.execute(f"""
        SELECT
            av.cod_cliente,
            av.nom_cliente,
            av.canal,
            av.cod_vendedor,
            av.venta_actual,
            av.objetivo,
            av.objetivo_pesos,
            av.objetivo_premium_pesos,
            dc.ciudad,
            dc.direccion,
            dc.lat,
            dc.lon,
            COALESCE(ca.nivel, 'ESTANDAR') as nivel,
            COALESCE(ca.estado, 'ACTIVO') as estado,
            ca.contacto_nombre,
            ca.contacto_telefono,
            ca.frecuencia_visita,
            ca.notas_cuenta,
            cp.ponderacion_pct as ponderacion_custom,
            (SELECT fecha FROM crm_gestiones g WHERE g.cod_cliente = av.cod_cliente ORDER BY fecha DESC LIMIT 1) as ultima_gestion,
            (SELECT tipo FROM crm_gestiones g WHERE g.cod_cliente = av.cod_cliente ORDER BY fecha DESC LIMIT 1) as ultima_gestion_tipo,
            (SELECT COUNT(*) FROM crm_gestiones g WHERE g.cod_cliente = av.cod_cliente) as total_gestiones,
            (SELECT COUNT(*) FROM crm_compromisos c WHERE c.cod_cliente = av.cod_cliente AND c.estado = 'PENDIENTE') as compromisos_pendientes,
            (SELECT COUNT(*) FROM crm_pdv p WHERE p.cod_cliente = av.cod_cliente AND p.activo = 1) as pdvs_activos
        FROM fact_avance_cliente_vendedor_month av
        LEFT JOIN dim_clients dc ON av.cod_cliente = dc.cliente_id
        LEFT JOIN crm_accounts ca ON av.cod_cliente = ca.cod_cliente
        LEFT JOIN crm_cliente_ponderacion cp ON cp.cod_cliente = av.cod_cliente AND cp.year_month = ?
        WHERE {where}
        ORDER BY COALESCE(ca.nivel, 'ESTANDAR') DESC, av.nom_cliente ASC
    """, [year_month] + params).fetchall()

    result = []
    for r in rows:
        d = dict(r)
        # Ponderación: custom if set, else estratégica (objetivo/total*100)
        if d.get('ponderacion_custom') is not None:
            pond = float(d['ponderacion_custom'])
        elif total_objetivo > 0 and (d.get('objetivo') or 0) > 0:
            pond = round((d['objetivo'] / total_objetivo) * 100, 1)
        else:
            pond = 0
        d['ponderacion'] = pond
        d['ponderacion_editable'] = d.get('ponderacion_custom') is not None
        d['total_objetivo'] = total_objetivo
        d['total_objetivo_pesos'] = total_objetivo_pesos
        d['total_objetivo_premium'] = total_objetivo_premium
        result.append(d)

    conn.close()
    return jsonify(result)


@app.route('/api/crm/ponderacion/<cod_cliente>', methods=['PUT'])
@login_required
def api_crm_ponderacion(cod_cliente):
    """Set custom ponderación (weight %) for a client. Affects all objectives. Send null to revert to default."""
    data = request.json or {}
    ponderacion = data.get('ponderacion_pct')
    conn = get_db()
    ym_row = conn.execute("SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month").fetchone()
    year_month = ym_row[0] if ym_row and ym_row[0] else datetime.now().strftime('%Y-%m')

    if ponderacion is None:
        conn.close()
        return jsonify({'error': 'ponderacion_pct requerido (o revert: true para restaurar)'}), 400
    try:
        pond = float(ponderacion)
        if pond < 0 or pond > 100:
            return jsonify({'error': 'ponderacion debe estar entre 0 y 100'}), 400
    except (TypeError, ValueError):
        conn.close()
        return jsonify({'error': 'ponderacion debe ser un número'}), 400

    conn.execute("""
        INSERT INTO crm_cliente_ponderacion (cod_cliente, year_month, ponderacion_pct, updated_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(cod_cliente, year_month) DO UPDATE SET
            ponderacion_pct = excluded.ponderacion_pct,
            updated_at = CURRENT_TIMESTAMP
    """, (cod_cliente, year_month, pond))
    conn.commit()

    # Recalculate and update fact_avance objectives for this client
    avance_row = conn.execute("""
        SELECT cod_vendedor FROM fact_avance_cliente_vendedor_month
        WHERE cod_cliente = ? AND year_month = ?
        LIMIT 1
    """, (cod_cliente, year_month)).fetchone()

    if avance_row:
        cod_ven = avance_row['cod_vendedor']
        # Use fact_avance sum as source of truth (matches portfolio total)
        sum_row = conn.execute("""
            SELECT SUM(objetivo) as t_kg, SUM(objetivo_pesos) as t_pesos, SUM(objetivo_premium_pesos) as t_premium
            FROM fact_avance_cliente_vendedor_month
            WHERE cod_vendedor = ? AND year_month = ?
        """, (cod_ven, year_month)).fetchone()
        if sum_row and (sum_row['t_kg'] or 0) > 0:
            v = dict(sum_row)
            pct = pond / 100.0
            t_kg = v.get('t_kg') or 0
            t_pesos = v.get('t_pesos') or 0
            t_premium = v.get('t_premium') or 0
            new_objetivo = t_kg * pct
            new_pesos = t_pesos * pct if t_pesos else 0
            new_premium = t_premium * pct if t_premium else 0
            conn.execute("""
                UPDATE fact_avance_cliente_vendedor_month
                SET objetivo = ?, objetivo_pesos = ?, objetivo_premium_pesos = ?
                WHERE cod_cliente = ? AND year_month = ?
            """, (new_objetivo, new_pesos, new_premium, cod_cliente, year_month))
            conn.commit()

    conn.close()
    return jsonify({'status': 'success', 'ponderacion_pct': pond})


@app.route('/api/crm/account/<cod_cliente>', methods=['GET', 'PUT'])
@login_required
def api_crm_account(cod_cliente):
    """Get or update CRM enrichment for a specific account."""
    conn = get_db()
    if request.method == 'PUT':
        data = request.json
        conn.execute("""
            INSERT INTO crm_accounts (cod_cliente, nivel, estado, contacto_nombre, contacto_telefono,
                contacto_email, frecuencia_visita, notas_cuenta, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(cod_cliente) DO UPDATE SET
                nivel = excluded.nivel,
                estado = excluded.estado,
                contacto_nombre = excluded.contacto_nombre,
                contacto_telefono = excluded.contacto_telefono,
                contacto_email = excluded.contacto_email,
                frecuencia_visita = excluded.frecuencia_visita,
                notas_cuenta = excluded.notas_cuenta,
                updated_at = CURRENT_TIMESTAMP
        """, (cod_cliente, data.get('nivel'), data.get('estado'), data.get('contacto_nombre'),
              data.get('contacto_telefono'), data.get('contacto_email'),
              data.get('frecuencia_visita'), data.get('notas_cuenta')))
        conn.commit()
        conn.close()
        return jsonify({'status': 'success'})

    # GET — full account detail
    row = conn.execute("""
        SELECT dc.*, COALESCE(ca.nivel,'ESTANDAR') as nivel, COALESCE(ca.estado,'ACTIVO') as estado,
               ca.contacto_nombre, ca.contacto_telefono, ca.contacto_email,
               ca.frecuencia_visita, ca.notas_cuenta
        FROM dim_clients dc
        LEFT JOIN crm_accounts ca ON dc.cliente_id = ca.cod_cliente
        WHERE dc.cliente_id = ?
    """, (cod_cliente,)).fetchone()
    conn.close()
    return jsonify(dict(row) if row else {})


@app.route('/api/crm/gestiones/<cod_cliente>', methods=['GET', 'POST'])
@login_required
def api_crm_gestiones(cod_cliente):
    """Get or log gestiones (interactions) for a specific account."""
    conn = get_db()
    if request.method == 'POST':
        data = request.json
        conn.execute("""
            INSERT INTO crm_gestiones (cod_cliente, contacto, tipo, fecha, resultado, compromisos, proximo_paso, proximo_paso_fecha)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (cod_cliente, data.get('contacto'), data.get('tipo'), data.get('fecha'),
              data.get('resultado'), data.get('compromisos'),
              data.get('proximo_paso'), data.get('proximo_paso_fecha')))
        conn.commit()
        conn.close()
        return jsonify({'status': 'success'}), 201

    rows = conn.execute("""
        SELECT * FROM crm_gestiones WHERE cod_cliente = ? ORDER BY fecha DESC
    """, (cod_cliente,)).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/crm/compromisos/<cod_cliente>', methods=['GET', 'POST', 'PUT'])
@login_required
def api_crm_compromisos(cod_cliente):
    """Manage formal commitments for an account."""
    conn = get_db()
    if request.method == 'POST':
        data = request.json
        conn.execute("""
            INSERT INTO crm_compromisos (cod_cliente, periodo, tipo, descripcion, valor_acordado, estado)
            VALUES (?, ?, ?, ?, ?, 'PENDIENTE')
        """, (cod_cliente, data.get('periodo'), data.get('tipo'),
              data.get('descripcion'), data.get('valor_acordado')))
        conn.commit()
        conn.close()
        return jsonify({'status': 'success'}), 201

    if request.method == 'PUT':
        data = request.json
        conn.execute("""
            UPDATE crm_compromisos SET estado = ?, valor_real = ? WHERE id = ?
        """, (data.get('estado'), data.get('valor_real'), data.get('id')))
        conn.commit()
        conn.close()
        return jsonify({'status': 'success'})

    rows = conn.execute("""
        SELECT * FROM crm_compromisos WHERE cod_cliente = ? ORDER BY periodo DESC
    """, (cod_cliente,)).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/crm/planificacion', methods=['GET', 'POST', 'PUT'])
@login_required
def api_crm_planificacion():
    """Executive planning: monthly, weekly, daily."""
    conn = get_db()
    tipo = request.args.get('tipo', 'DIARIA')
    fecha = request.args.get('fecha', '')

    if request.method == 'POST':
        data = request.json
        conn.execute("""
            INSERT INTO crm_planificacion (tipo, fecha, cod_cliente, objetivo)
            VALUES (?, ?, ?, ?)
        """, (data.get('tipo'), data.get('fecha'), data.get('cod_cliente'), data.get('objetivo')))
        conn.commit()
        conn.close()
        return jsonify({'status': 'success'}), 201

    if request.method == 'PUT':
        data = request.json
        conn.execute("""
            UPDATE crm_planificacion SET completado = ?, resultado = ? WHERE id = ?
        """, (data.get('completado', 0), data.get('resultado'), data.get('id')))
        conn.commit()
        conn.close()
        return jsonify({'status': 'success'})

    query = "SELECT p.*, dc.cliente_name as nom_cliente FROM crm_planificacion p LEFT JOIN dim_clients dc ON p.cod_cliente = dc.cliente_id WHERE p.tipo = ?"
    params = [tipo]
    if fecha:
        query += " AND p.fecha = ?"; params.append(fecha)
    query += " ORDER BY p.completado ASC, p.fecha ASC"

    rows = conn.execute(query, params).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/crm/planificacion-recurrente', methods=['GET', 'POST'])
@app.route('/api/crm/planificacion-recurrente/<int:rid>', methods=['PUT', 'DELETE'])
@login_required
def api_crm_planificacion_recurrente(rid=None):
    """Recurring planning rules (e.g. every Friday load orders for MUY BARATO)."""
    conn = get_db()

    if request.method == 'POST':
        data = request.json
        conn.execute("""
            INSERT INTO crm_planificacion_recurrente (cod_cliente, descripcion, dia_semana, activo)
            VALUES (?, ?, ?, ?)
        """, (data.get('cod_cliente'), data.get('descripcion'), data.get('dia_semana', 4), data.get('activo', 1)))
        conn.commit()
        conn.close()
        return jsonify({'status': 'success'}), 201

    if request.method == 'PUT' and rid:
        data = request.json
        conn.execute("""
            UPDATE crm_planificacion_recurrente
            SET cod_cliente=?, descripcion=?, dia_semana=?, activo=?
            WHERE id=?
        """, (data.get('cod_cliente'), data.get('descripcion'), data.get('dia_semana'), data.get('activo', 1), rid))
        conn.commit()
        conn.close()
        return jsonify({'status': 'success'})

    if request.method == 'DELETE' and rid:
        conn.execute("DELETE FROM crm_planificacion_recurrente WHERE id=?", (rid,))
        conn.execute("DELETE FROM crm_planificacion_recurrente_completado WHERE recurrente_id=?", (rid,))
        conn.commit()
        conn.close()
        return jsonify({'status': 'success'})

    # GET: list all recurring rules
    rows = conn.execute("""
        SELECT r.*, dc.cliente_name as nom_cliente
        FROM crm_planificacion_recurrente r
        LEFT JOIN dim_clients dc ON r.cod_cliente = dc.cliente_id
        WHERE r.activo = 1
        ORDER BY r.dia_semana, r.cod_cliente
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/crm/planificacion-recurrente/<int:rid>/completado', methods=['POST'])
@login_required
def api_crm_planificacion_recurrente_completado(rid):
    """Mark a recurring task as done for a specific date."""
    data = request.json or {}
    fecha = data.get('fecha')
    completado = data.get('completado', 1)
    resultado = data.get('resultado')
    if not fecha:
        return jsonify({'error': 'fecha required'}), 400
    conn = get_db()
    conn.execute("""
        INSERT OR REPLACE INTO crm_planificacion_recurrente_completado (recurrente_id, fecha, completado, resultado)
        VALUES (?, ?, ?, ?)
    """, (rid, fecha, completado, resultado))
    conn.commit()
    conn.close()
    return jsonify({'status': 'success'})


@app.route('/api/crm/planificacion-recurrente-para-fecha')
@login_required
def api_crm_planificacion_recurrente_para_fecha():
    """Get recurring tasks that apply to a given date (for merging into daily agenda)."""
    fecha = request.args.get('fecha')
    if not fecha:
        return jsonify([])
    try:
        d = datetime.strptime(fecha, '%Y-%m-%d')
        dia_semana = d.weekday()  # 0=Monday, 6=Sunday
    except ValueError:
        return jsonify([])
    conn = get_db()
    rows = conn.execute("""
        SELECT r.*, dc.cliente_name as nom_cliente,
               c.completado as completado_fecha, c.resultado as resultado_fecha
        FROM crm_planificacion_recurrente r
        LEFT JOIN dim_clients dc ON r.cod_cliente = dc.cliente_id
        LEFT JOIN crm_planificacion_recurrente_completado c
          ON c.recurrente_id = r.id AND c.fecha = ?
        WHERE r.activo = 1 AND r.dia_semana = ?
        ORDER BY r.cod_cliente
    """, (fecha, dia_semana)).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/crm/pdv/<cod_cliente>', methods=['GET', 'POST'])
@login_required
def api_crm_pdv(cod_cliente):
    """Get or create PDVs for a distributor account."""
    conn = get_db()
    if request.method == 'POST':
        data = request.json
        conn.execute("""
            INSERT INTO crm_pdv (cod_cliente, nombre, direccion, ciudad, lat, lon)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (cod_cliente, data.get('nombre'), data.get('direccion'),
              data.get('ciudad'), data.get('lat'), data.get('lon')))
        conn.commit()
        conn.close()
        return jsonify({'status': 'success'}), 201

    rows = conn.execute("""
        SELECT * FROM crm_pdv WHERE cod_cliente = ? AND activo = 1 ORDER BY nombre
    """, (cod_cliente,)).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/crm/tasks')
def api_crm_tasks():
    """Generate real proximity and performance alerts (gestiones, cold, desavance). Each has alert_id for dismiss."""
    vendedor = request.args.get('vendedor', '')
    conn = get_db()

    # 1. Overdue/Next Steps from Gestiones
    gestiones_tasks = conn.execute("""
        SELECT 
            g.id,
            'GESTION' as tipo,
            'Próximo Paso: ' || proximo_paso as descripcion,
            proximo_paso_fecha as fecha_vencimiento,
            'ALTA' as prioridad,
            cod_cliente,
            (SELECT cliente_name FROM dim_clients WHERE cliente_id = g.cod_cliente) as nom_cliente
        FROM crm_gestiones g
        WHERE proximo_paso_fecha <= date('now', '+2 days')
          AND proximo_paso_fecha IS NOT NULL
        ORDER BY proximo_paso_fecha ASC
        LIMIT 10
    """).fetchall()

    # 2. Clients "Cold" (No contact > 20 days)
    cold_clients = conn.execute("""
        SELECT 
            'FALTA_GESTION' as tipo,
            'Sin gestión hace > 20 días' as descripcion,
            MAX(fecha) as fecha_vencimiento,
            'MEDIA' as prioridad,
            cod_cliente,
            (SELECT cliente_name FROM dim_clients WHERE cliente_id = g.cod_cliente) as nom_cliente
        FROM crm_gestiones g
        GROUP BY cod_cliente
        HAVING fecha_vencimiento < date('now', '-20 days')
        LIMIT 5
    """).fetchall()

    # 3. Critical Sales Gap
    gap_alerts = conn.execute("""
        SELECT 
            'DESAVANCE' as tipo,
            'Bajo cumplimiento: ' || CAST(ROUND((venta_actual/objetivo)*100) AS INTEGER) || '%' as descripcion,
            date('now') as fecha_vencimiento,
            'BAJA' as prioridad,
            cod_cliente,
            nom_cliente
        FROM fact_avance_cliente_vendedor_month
        WHERE objetivo > 0 AND (venta_actual/objetivo) < 0.4
          AND year_month = (SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month)
        LIMIT 5
    """).fetchall()

    all_alerts = []
    for r in gestiones_tasks:
        d = dict(r)
        d['alert_id'] = f"GESTION_{d['cod_cliente']}_{d.get('fecha_vencimiento','')}_{d.get('id','')}"
        all_alerts.append(d)
    for r in cold_clients:
        d = dict(r)
        d['alert_id'] = f"FALTA_GESTION_{d['cod_cliente']}_{d.get('fecha_vencimiento','')}"
        all_alerts.append(d)
    for r in gap_alerts:
        d = dict(r)
        d['alert_id'] = f"DESAVANCE_{d['cod_cliente']}_{d.get('fecha_vencimiento','')}"
        all_alerts.append(d)

    all_alerts = _filter_dismissed(conn, all_alerts)
    conn.close()
    return jsonify(all_alerts)



# ==================== BI & PRICING API ENDPOINTS ====================

@app.route('/api/bi/pricing/comparative')
def api_bi_pricing_comparative():

    """Comparative analysis of our PPA vs Competition."""
    canal = request.args.get('canal', 'DH')
    conn = get_db()
    
    # This logic assumes there's competition data loaded or derived.
    # For now, let's provide a skeleton that queries verbas and prices_list.
    rows = conn.execute("""
        SELECT v.sku, p.descripcion, v.ppa, v.precio_g, v.dcto_f, v.periodo_desde
        FROM verbas v
        JOIN dim_product_classification p ON v.sku = p.cod_producto
        WHERE v.canal = ?
        ORDER BY v.periodo_desde DESC
    """, (canal,)).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/bi/ppl/analisis')
@login_required
def api_bi_ppl_analisis():
    """P&L Analysis: Verba vs Real Billing."""
    canal = request.args.get('canal')
    conn = get_db()
    
    # Complex join between facturacion and verbas (per period)
    # This is a high-level summary.
    query = """
        SELECT 
            f.year_month,
            f.cod_vendedor,
            SUM(f.importe) as facturacion_real,
            SUM(f.cantidad) as kg_reales,
            AVG(v.precio_g) as precio_acordado_avg
        FROM fact_facturacion f
        LEFT JOIN verbas v ON f.cod_producto = v.sku AND f.year_month >= v.periodo_desde AND f.year_month <= v.periodo_hasta
    """
    if canal:
        query += " WHERE v.canal = ?"
        rows = conn.execute(query + " GROUP BY f.year_month, f.cod_vendedor", (canal,)).fetchall()
    else:
        rows = conn.execute(query + " GROUP BY f.year_month, f.cod_vendedor").fetchall()
        
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/bi/rotation')
def api_bi_rotation():
    """Analyze categories with lowest coverage (market penetration) as alerts."""
    conn = get_db()
    rows = conn.execute("""
        SELECT 
            p.categoria,
            SUM(f.cantidad) as total_kg,
            COUNT(DISTINCT f.cod_cliente) as compradores,
            (SELECT COUNT(*) FROM dim_clients) as total_universo,
            (CAST(COUNT(DISTINCT f.cod_cliente) AS REAL) / (SELECT COUNT(*) FROM dim_clients)) as coverage
        FROM fact_facturacion f
        LEFT JOIN dim_product_classification p ON f.cod_producto = p.cod_producto
        WHERE f.year_month = (SELECT MAX(year_month) FROM fact_facturacion)
        GROUP BY p.categoria
        HAVING total_kg > 100 -- Avoid tiny categories
        ORDER BY coverage ASC
        LIMIT 5
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])
    


# ─── COBERTURAS ────────────────────────────────────────

@app.route('/mapa')
@login_required
def mapa_page():
    return render_template('mapa.html')


@app.route('/api/mapa/clientes')
def api_mapa_clientes():
    """Return all clients with location data and current month stats for the map."""
    cod_vendedor = request.args.get('vendedor', '')
    jefe = request.args.get('jefe', '')
    zona = request.args.get('zona', '')

    conn = get_db()

    where_parts = ["av.year_month = (SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month)"]
    params = []
    if cod_vendedor:
        where_parts.append("av.cod_vendedor = ?")
        params.append(cod_vendedor)
    elif jefe:
        where_parts.append("av.jefe = ?")
        params.append(jefe)
    elif zona:
        where_parts.append("av.zona = ?")
        params.append(zona)

    where = " AND ".join(where_parts)

    rows = conn.execute(f"""
        SELECT
            av.cod_cliente,
            av.nom_cliente,
            av.cod_vendedor,
            av.nom_vendedor,
            av.frecuencia,
            av.venta_actual,
            av.objetivo,
            av.pendiente,
            dc.ciudad,
            dc.provincia,
            dc.direccion,
            dc.telefono,
            dc.correo,
            dc.canal,
            dc.lat,
            dc.lon,
            s.tier
        FROM fact_avance_cliente_vendedor_month av
        LEFT JOIN dim_clients dc ON av.cod_cliente = dc.cliente_id
        LEFT JOIN fact_client_segmentation s ON av.cod_cliente = s.cod_cliente AND av.year_month = s.year_month
        WHERE {where}
        ORDER BY av.nom_cliente
    """, params).fetchall()

    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/mapa/cliente/<cod_cliente>/geocode', methods=['POST'])
def api_save_geocode(cod_cliente):
    """Save geocoded lat/lon for a client."""
    data = request.get_json()
    lat = data.get('lat')
    lon = data.get('lon')
    if lat is None or lon is None:
        return jsonify({'error': 'lat and lon required'}), 400
    conn = get_db()
    conn.execute("UPDATE dim_clients SET lat=?, lon=? WHERE cliente_id=?", (lat, lon, cod_cliente))
    conn.commit()
    conn.close()
    return jsonify({'ok': True})


@app.route('/coberturas')
def coberturas_page():
    return render_template('coberturas.html')


@app.route('/api/insights')
def api_insights():
    """
    Generate automatic intelligence insights for the dashboard:
    - forecast: month-end projection with confidence interval
    - risk: high-value clients who bought last month but not this month
    - opportunities: top clients ranked by priority score (tier × pendiente)
    - new_buyers: clients that bought for the first time this month (no history)
    """
    cod_vendedor = request.args.get('vendedor', '')
    jefe = request.args.get('jefe', '')
    zona = request.args.get('zona', '')

    if not any([cod_vendedor, jefe, zona]):
        return jsonify({'error': 'filter required'}), 400

    conn = get_db()

    # Build WHERE clause
    if cod_vendedor:
        where = "av.cod_vendedor = ?"
        params = [cod_vendedor]
        vendor_where = "cod_vendedor = ?"
        vendor_params = [cod_vendedor]
    elif jefe:
        where = "av.jefe = ?"
        params = [jefe]
        vendor_where = "jefe = ?"
        vendor_params = [jefe]
    else:
        where = "av.zona = ?"
        params = [zona]
        vendor_where = "zona = ?"
        vendor_params = [zona]

    # Get active year_month
    ym_row = conn.execute(
        "SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month"
    ).fetchone()
    cur_ym = (ym_row[0] if ym_row else None) or datetime.now().strftime('%Y-%m')
    year, month = map(int, cur_ym.split('-'))

    # Previous month
    prev_dt = datetime(year, month, 1) - timedelta(days=1)
    prev_ym = prev_dt.strftime('%Y-%m')

    # ── 1. FORECAST ─────────────────────────────────────────────────
    summary = conn.execute(f"""
        SELECT SUM(av.venta_actual) as fact, SUM(av.pendiente) as pend,
               SUM(av.objetivo) as obj
        FROM fact_avance_cliente_vendedor_month av
        WHERE {where} AND av.year_month = ?
    """, params + [cur_ym]).fetchone()

    fact_kg = summary['fact'] or 0
    pend_kg = summary['pend'] or 0
    obj_kg  = summary['obj'] or 0

    # Daily sales from fact_facturacion for variance
    vendor_codes_rows = conn.execute(f"""
        SELECT DISTINCT cod_vendedor FROM fact_avance_cliente_vendedor_month
        WHERE {vendor_where} AND year_month = ?
    """, vendor_params + [cur_ym]).fetchall()
    vendor_codes = [r['cod_vendedor'] for r in vendor_codes_rows]

    import statistics as _stats

    days_in_month = calendar.monthrange(year, month)[1]
    today = datetime.now()
    is_cur_month = (cur_ym == today.strftime('%Y-%m'))
    elapsed_days = today.day if is_cur_month else days_in_month
    # Días restantes = días hábiles (lunes a viernes) hasta fin de mes
    remaining_days = 0
    if is_cur_month:
        current = today.date()
        month_end = datetime(year, month, days_in_month).date()
        while current <= month_end:
            if current.weekday() < 5:  # 0=Mon, 4=Fri
                remaining_days += 1
            current += timedelta(days=1)
    remaining_days = max(0, remaining_days)

    # ── Current-month in-progress projection (linear) ────────────
    # Uses business days for consistency with chart and daily_needed
    daily_variance = 0
    projected_kg_linear = fact_kg
    if vendor_codes and elapsed_days > 0:
        ph = ','.join(['?'] * len(vendor_codes))
        daily_rows = conn.execute(f"""
            SELECT strftime('%d', fecha_emision) as dia, SUM(cantidad) as kg
            FROM fact_facturacion
            WHERE cod_vendedor IN ({ph}) AND year_month = ?
            GROUP BY dia
        """, vendor_codes + [cur_ym]).fetchall()
        daily_vals = [r['kg'] for r in daily_rows if r['kg']]
        if daily_vals:
            # avg_daily = kg per business day (align with chart)
            bd_elapsed = sum(1 for d in range(1, today.day + 1)
                             if datetime(year, month, d).weekday() < 5) if is_cur_month else elapsed_days
            bd_elapsed = max(bd_elapsed, 1)
            # Use fact_kg (fact_avance) as base; rate from fact_facturacion when available
            total_from_fact = sum(daily_vals)
            avg_daily = total_from_fact / bd_elapsed
            if len(daily_vals) > 1:
                daily_variance = _stats.stdev(daily_vals)
            projected_kg_linear = fact_kg + avg_daily * remaining_days

    # ── Next-month multi-factor forecast (algorithm) ─────────────
    next_m_num = month + 1 if month < 12 else 1
    next_y_num = year if month < 12 else year + 1
    next_ym_fc = f"{next_y_num}-{next_m_num:02d}"

    # Aggregate historical monthly totals for the whole filter scope
    scope_hist_rows = conn.execute(f"""
        SELECT h.year_month, SUM(h.kg_vendidos) as kg
        FROM fact_cliente_historico h
        JOIN fact_avance_cliente_vendedor_month av
          ON h.cod_cliente = av.cod_cliente AND av.year_month = ?
        WHERE {where} AND h.kg_vendidos > 0
        GROUP BY h.year_month ORDER BY h.year_month
    """, params + [cur_ym]).fetchall()
    scope_hist = [(r['year_month'], r['kg']) for r in scope_hist_rows]

    next_forecast_kg = None
    next_low_kg = None
    next_high_kg = None
    next_confidence = None

    if scope_hist:
        scope_vals = [kg for _, kg in scope_hist]
        scope_months = [ym2 for ym2, _ in scope_hist]

        # Baseline: trimmed 6M mean
        r6 = scope_vals[-6:]
        if len(r6) >= 4:
            baseline_fc = _stats.mean(sorted(r6)[1:-1])
        else:
            baseline_fc = _stats.mean(r6)

        # Trend: slope last 3 months
        r3 = scope_vals[-3:]
        n3 = len(r3)
        if n3 >= 2:
            xm3 = _stats.mean(range(n3))
            ym3 = _stats.mean(r3)
            denom3 = sum((xi - xm3)**2 for xi in range(n3))
            slope3 = sum((xi-xm3)*(yi-ym3) for xi,yi in zip(range(n3),r3)) / denom3 if denom3 else 0
            trend_fc = 1.0 + max(-0.30, min(0.30, slope3 / baseline_fc))
        else:
            trend_fc = 1.0

        # Seasonality: same month in prior data
        same_m = [kg for ym2, kg in scope_hist if int(ym2.split('-')[1]) == next_m_num]
        all_mean_fc = _stats.mean(scope_vals) if scope_vals else baseline_fc
        season_fc = _stats.mean(same_m) / all_mean_fc if same_m and all_mean_fc > 0 else 1.0
        season_fc = max(0.65, min(1.50, season_fc))

        # Recency: did the scope sell last month?
        recency_fc = 1.0 if (scope_vals and scope_vals[-1] > 0) else 0.85

        next_forecast_kg = round(max(0, baseline_fc * trend_fc * season_fc * recency_fc), 0)
        std_fc = _stats.stdev(scope_vals) if len(scope_vals) >= 2 else baseline_fc * 0.20
        next_low_kg  = round(max(0, next_forecast_kg - std_fc), 0)
        next_high_kg = round(next_forecast_kg + std_fc, 0)
        next_confidence = min(85, 30 + 5 * len(scope_vals))

    # ── SITUACIÓN: Análisis semanal vs histórico ─────────────────
    situacion = {'semanas': [], 'resumen': '', 'alerta': None}
    proyeccion_ratio = None  # ratio-based projection
    escenarios_similares = []

    if vendor_codes and is_cur_month and today.day >= 7:
        ph = ','.join(['?'] * len(vendor_codes))
        # Current month weekly (from fact_facturacion)
        daily_cur = conn.execute(f"""
            SELECT CAST(strftime('%d', fecha_emision) AS INTEGER) as dia, SUM(cantidad) as kg
            FROM fact_facturacion
            WHERE cod_vendedor IN ({ph}) AND year_month = ?
            GROUP BY dia
        """, vendor_codes + [cur_ym]).fetchall()
        cur_by_day = {r['dia']: r['kg'] or 0 for r in daily_cur}

        # Historical: last 12 months daily for same vendors
        hist_months = conn.execute("""
            SELECT DISTINCT year_month FROM fact_facturacion
            WHERE cod_vendedor IN ({ph}) AND year_month != ?
            ORDER BY year_month DESC LIMIT 12
        """.format(ph=ph), vendor_codes + [cur_ym]).fetchall()
        hist_ym_list = [r['year_month'] for r in hist_months]

        hist_weekly = {}
        hist_ratio_by_day = {}
        hist_month_totals = []  # [(ym, total, acum_at_day_n), ...]

        for hym in hist_ym_list:
            rows = conn.execute(f"""
                SELECT CAST(strftime('%d', fecha_emision) AS INTEGER) as dia, SUM(cantidad) as kg
                FROM fact_facturacion
                WHERE cod_vendedor IN ({ph}) AND year_month = ?
                GROUP BY dia
            """, vendor_codes + [hym]).fetchall()
            by_day = {r['dia']: r['kg'] or 0 for r in rows}
            total_mes = sum(by_day.values())
            if total_mes <= 0:
                continue
            acum = 0
            for d in range(1, 32):
                acum += by_day.get(d, 0)
                if d not in hist_ratio_by_day:
                    hist_ratio_by_day[d] = []
                hist_ratio_by_day[d].append(acum / total_mes)
            hist_month_totals.append((hym, total_mes, sum(by_day.get(d, 0) for d in range(1, today.day + 1))))
            for sem in range(1, 6):
                kg_sem = sum(by_day.get(d, 0) for d in range((sem-1)*7+1, min(sem*7+1, 32)))
                hist_weekly.setdefault(sem, []).append(kg_sem)

        # Current month weeks
        for sem in range(1, 6):
            kg_sem_cur = sum(cur_by_day.get(d, 0) for d in range((sem-1)*7+1, min(sem*7+1, 32)))
            if (sem-1)*7+1 > today.day:
                break
            prom_hist = _stats.mean(hist_weekly.get(sem, [0])) if hist_weekly.get(sem) else 0
            diff_pct = round((kg_sem_cur - prom_hist) / prom_hist * 100, 1) if prom_hist else 0
            situacion['semanas'].append({
                'semana': sem,
                'kg': round(kg_sem_cur, 0),
                'prom_historico': round(prom_hist, 0),
                'diff_pct': diff_pct,
            })

        # Resumen situacional
        if len(situacion['semanas']) >= 2:
            s1, s2 = situacion['semanas'][0], situacion['semanas'][1]
            if s2['kg'] > 0 and s1['kg'] > 0:
                wow = round((s2['kg'] - s1['kg']) / s1['kg'] * 100, 1)
                if wow < -15:
                    situacion['alerta'] = 'baja'
                    situacion['resumen'] = f"Semana 2 fue {abs(wow)}% más baja que semana 1. En meses con caídas similares, la recuperación requirió acelerar ventas en la última quincena."
                elif wow > 20:
                    situacion['resumen'] = f"Semana 2 subió {wow}% vs semana 1. Buen ritmo."
                else:
                    situacion['resumen'] = f"Ritmo estable entre semanas. Semana 2: {s2['diff_pct']}% vs promedio histórico."
            elif s2['diff_pct'] < -20:
                situacion['alerta'] = 'baja'
                situacion['resumen'] = f"Semana 2 está {abs(s2['diff_pct'])}% por debajo del promedio histórico. Revisá causas y priorizá clientes con mayor pendiente."

        # Proyección ratio-based: qué % del mes típicamente teníamos al día N
        if today.day in hist_ratio_by_day and hist_ratio_by_day[today.day]:
            ratio_prom = _stats.mean(hist_ratio_by_day[today.day])
            if ratio_prom > 0.02:
                proyeccion_ratio = round(fact_kg / ratio_prom, 0)
                # Escenarios similares: meses donde al día N teníamos ratio similar (±15%)
                for hym, total_mes, acum_n in hist_month_totals:
                    if total_mes > 0:
                        r_at_n = acum_n / total_mes
                        if abs(r_at_n - ratio_prom) < 0.15:
                            escenarios_similares.append({
                                'mes': hym,
                                'cierre_final': round(total_mes, 0),
                                'ratio_dia_n': round(r_at_n * 100, 1),
                            })
                escenarios_similares = escenarios_similares[:5]

    # ── Proyección final: blend linear + ratio cuando hay historial ─
    if proyeccion_ratio is not None and obj_kg > 0:
        # Usar el más conservador entre linear y ratio, o promedio ponderado
        blend = 0.5 * projected_kg_linear + 0.5 * proyeccion_ratio
        final_kg = round(blend, 0)
    elif not is_cur_month and pend_kg > 0:
        final_kg = fact_kg + pend_kg
    else:
        final_kg = projected_kg_linear

    proj_pct = round(final_kg / obj_kg * 100, 1) if obj_kg else 0
    conf_range = daily_variance * (remaining_days ** 0.5)
    low_kg  = max(0, projected_kg_linear - conf_range)
    high_kg = projected_kg_linear + conf_range
    low_pct  = round(low_kg / obj_kg * 100, 1) if obj_kg else 0
    high_pct = round(high_kg / obj_kg * 100, 1) if obj_kg else 0

    daily_needed = round((obj_kg - fact_kg) / remaining_days, 0) if remaining_days > 0 else 0

    # Plan de recuperación
    plan_recuperacion = []
    if obj_kg > 0 and fact_kg < obj_kg and remaining_days > 0:
        gap = obj_kg - fact_kg
        plan_recuperacion.append({
            'tipo': 'objetivo',
            'titulo': 'KG/día necesarios para alcanzar objetivo',
            'valor': round(daily_needed, 0),
            'detalle': f"{int(gap):,} kg en {remaining_days} días hábiles",
        })
        # Clientes prioritarios: poder de compra + ponderación + no compraron/compraron menos
        # Incluye: ponderación estratégica, kg históricos (poder compra), venta_actual vs histórico
        hist_months = conn.execute("""
            SELECT DISTINCT year_month FROM fact_cliente_historico
            WHERE year_month < ? ORDER BY year_month DESC LIMIT 6
        """, [cur_ym]).fetchall()
        hist_ym_list = [r['year_month'] for r in hist_months] or [prev_ym]
        hist_ph = ','.join(['?'] * len(hist_ym_list))
        where_av2 = where.replace("av.", "av2.")

        clientes_prioridad = conn.execute(f"""
            WITH poder_compra AS (
                SELECT cod_cliente, AVG(kg_vendidos) as avg_kg_hist
                FROM fact_cliente_historico
                WHERE year_month IN ({hist_ph})
                GROUP BY cod_cliente
            )
            SELECT
                av.cod_cliente,
                av.nom_cliente,
                av.pendiente,
                av.objetivo,
                av.venta_actual,
                COALESCE(cp.ponderacion_pct, (av.objetivo * 100.0 / NULLIF((SELECT SUM(objetivo) FROM fact_avance_cliente_vendedor_month av2 WHERE {where_av2} AND av2.year_month = ?), 0))) as ponderacion,
                COALESCE(pc.avg_kg_hist, 0) as poder_compra_kg
            FROM fact_avance_cliente_vendedor_month av
            LEFT JOIN crm_cliente_ponderacion cp ON av.cod_cliente = cp.cod_cliente AND cp.year_month = ?
            LEFT JOIN poder_compra pc ON av.cod_cliente = pc.cod_cliente
            WHERE {where} AND av.year_month = ? AND av.pendiente > 0
        """, hist_ym_list + params + [cur_ym, cur_ym] + params + [cur_ym]).fetchall()

        # Score: ponderación + poder compra + pendiente + bonus si no compró o compró menos
        scored = []
        if not clientes_prioridad:
            top_clientes = []
        else:
            max_pend = max((r['pendiente'] for r in clientes_prioridad), default=1)
            max_poder = max((r['poder_compra_kg'] or 0 for r in clientes_prioridad), default=1)
            for r in clientes_prioridad:
                pond = float(r['ponderacion'] or 0)
                poder = float(r['poder_compra_kg'] or 0)
                pend = float(r['pendiente'])
                venta = float(r['venta_actual'] or 0)
                no_compro = 1 if venta == 0 else 0
                compro_menos = 1 if poder > 0 and venta < poder * 0.7 else 0
                score = (
                    (pond / 100.0) * 25 +
                    min(poder / max_poder, 1.0) * 25 +
                    (pend / max_pend) * 35 +
                    no_compro * 15 +
                    compro_menos * 10
                )
                scored.append((score, r))
            scored.sort(key=lambda x: x[0], reverse=True)
            top_clientes = [s[1] for s in scored[:5]]

        if top_clientes:
            def _motivo(r):
                v = float(r['venta_actual'] or 0)
                p = float(r['poder_compra_kg'] or 0)
                if v == 0:
                    return 'no compró este mes'
                if p > 0 and v < p * 0.7:
                    return 'compró menos de lo habitual'
                return None
            plan_recuperacion.append({
                'tipo': 'prioridad',
                'titulo': 'Priorizar estos clientes',
                'clientes': [
                    {
                        'nombre': r['nom_cliente'],
                        'pendiente': round(r['pendiente'], 0),
                        'motivo': _motivo(r),
                    }
                    for r in top_clientes
                ],
                'detalle': 'Ponderación + poder de compra + pendiente · prioridad a quienes no compraron o compraron menos',
            })
        if situacion.get('alerta') == 'baja' and escenarios_similares:
            cierres = [e['cierre_final'] for e in escenarios_similares]
            plan_recuperacion.append({
                'tipo': 'referencia',
                'titulo': 'Escenarios similares en el pasado',
                'detalle': f"En {len(escenarios_similares)} mes(es) con ritmo similar: cierre promedio {round(_stats.mean(cierres), 0):,} kg",
            })

    # Trend vs last month
    prev_total_row = conn.execute(f"""
        SELECT SUM(h.kg_vendidos) as kg
        FROM fact_cliente_historico h
        JOIN fact_avance_cliente_vendedor_month av
          ON h.cod_cliente = av.cod_cliente
        WHERE {where} AND av.year_month = ?
          AND h.year_month = ?
    """, params + [cur_ym, prev_ym]).fetchone()
    prev_kg = prev_total_row['kg'] or 0
    trend_vs_prev = round((fact_kg - prev_kg) / prev_kg * 100, 1) if prev_kg else None

    forecast = {
        # Current month progress
        'projected_kg': round(final_kg, 0),
        'proj_pct': proj_pct,
        'low_pct': low_pct,
        'high_pct': high_pct,
        'fact_kg': round(fact_kg, 0),
        'obj_kg': round(obj_kg, 0),
        'pend_kg': round(pend_kg, 0),
        'days_remaining': remaining_days,
        'elapsed_days': elapsed_days,
        'daily_needed': daily_needed,
        'trend_vs_prev_pct': trend_vs_prev,
        'is_month_closed': not is_cur_month,
        'cur_ym': cur_ym,
        # Situación asistida (análisis semanal, escenarios)
        'situacion': situacion,
        'escenarios_similares': escenarios_similares,
        'plan_recuperacion': plan_recuperacion,
        'projected_kg_linear': round(projected_kg_linear, 0),
        'projected_kg_ratio': proyeccion_ratio,
        # Next month multi-factor forecast
        'next_ym': next_ym_fc,
        'next_forecast_kg': next_forecast_kg,
        'next_low_kg': next_low_kg,
        'next_high_kg': next_high_kg,
        'next_confidence': next_confidence,
        'next_proj_pct': round(next_forecast_kg / obj_kg * 100, 1) if (next_forecast_kg and obj_kg) else None,
    }

    # ── 2. RISK — clients who bought last month but 0 this month ────
    tier_weight = {'AAA': 4, 'AA': 3, 'A': 2, 'B': 1, 'CN': 3}

    risk_rows = conn.execute(f"""
        SELECT
            av.cod_cliente,
            av.nom_cliente,
            av.objetivo,
            av.venta_actual,
            s.tier,
            h.kg_vendidos as kg_prev_month,
            dc.telefono,
            dc.contacto
        FROM fact_avance_cliente_vendedor_month av
        LEFT JOIN fact_client_segmentation s
            ON av.cod_cliente = s.cod_cliente AND av.year_month = s.year_month
        LEFT JOIN fact_cliente_historico h
            ON av.cod_cliente = h.cod_cliente AND h.year_month = ?
        LEFT JOIN dim_clients dc ON av.cod_cliente = dc.cliente_id
        WHERE {where}
          AND av.year_month = ?
          AND av.venta_actual = 0
          AND h.kg_vendidos > 0
        ORDER BY (CASE s.tier WHEN 'AAA' THEN 4 WHEN 'AA' THEN 3 WHEN 'CN' THEN 3 WHEN 'A' THEN 2 ELSE 1 END) DESC,
                 h.kg_vendidos DESC
        LIMIT 8
    """, [prev_ym] + params + [cur_ym]).fetchall()

    risk = []
    for r in risk_rows:
        d = dict(r)
        d['telefono'] = str(d['telefono']).replace('.0','') if d['telefono'] else None
        risk.append(d)

    # ── 3. OPPORTUNITIES — solo clientes visitables HOY (por día + frecuencia) ──
    # Mismo mapeo que planificación: Lun→Mar,Mié; Mar→Mié,Jue; Mié→Jue,Vie; Jue→Vie,Lun; Vie→Lun,Mar; Sáb/Dom→ninguno
    freq_map = {0: ['MARTES', 'MIERCOLES'], 1: ['MIERCOLES', 'JUEVES'], 2: ['JUEVES', 'VIERNES'],
                3: ['VIERNES', 'LUNES'], 4: ['LUNES', 'MARTES'], 5: [], 6: []}
    target_frecuencias = freq_map.get(today.weekday(), [])
    opp_rows = []
    if target_frecuencias:
        freq_cond = " OR ".join(["UPPER(av.frecuencia) LIKE ?" for _ in target_frecuencias])
        freq_params = [f"%{f}%" for f in target_frecuencias]
        opp_rows = conn.execute(f"""
            SELECT
                av.cod_cliente,
                av.nom_cliente,
                av.venta_actual,
                av.pendiente,
                av.objetivo,
                av.frecuencia,
                s.tier,
                dc.telefono,
                dc.contacto
            FROM fact_avance_cliente_vendedor_month av
            LEFT JOIN fact_client_segmentation s
                ON av.cod_cliente = s.cod_cliente AND av.year_month = s.year_month
            LEFT JOIN dim_clients dc ON av.cod_cliente = dc.cliente_id
            WHERE {where}
              AND av.year_month = ?
              AND av.venta_actual > 0
              AND av.pendiente > 0
              AND ({freq_cond})
            ORDER BY
                (CASE s.tier WHEN 'AAA' THEN 4 WHEN 'AA' THEN 3 WHEN 'CN' THEN 3 WHEN 'A' THEN 2 ELSE 1 END) DESC,
                av.pendiente DESC
            LIMIT 8
        """, params + [cur_ym] + freq_params).fetchall()

    opportunities = []
    for r in opp_rows:
        d = dict(r)
        pct = round(d['venta_actual'] / d['objetivo'] * 100, 1) if d['objetivo'] else 0
        tw = tier_weight.get(d['tier'], 1)
        # Priority score: weighted combination of tier + % remaining + abs pendiente
        pct_remaining = max(0, 100 - pct) / 100
        norm_pend = min(d['pendiente'] / 5000, 1.0)  # normalize vs 5000 KG cap
        d['priority_score'] = round((tw / 4 * 0.4 + pct_remaining * 0.4 + norm_pend * 0.2) * 100)
        d['pct_objetivo'] = pct
        d['telefono'] = str(d['telefono']).replace('.0','') if d['telefono'] else None
        opportunities.append(d)

    # Sort by priority score descending
    opportunities.sort(key=lambda x: x['priority_score'], reverse=True)

    # ── 4. NEW BUYERS — first purchase this month ───────────────────
    new_buyers_rows = conn.execute(f"""
        SELECT
            av.cod_cliente,
            av.nom_cliente,
            av.venta_actual as kg_actual,
            s.tier
        FROM fact_avance_cliente_vendedor_month av
        LEFT JOIN fact_client_segmentation s
            ON av.cod_cliente = s.cod_cliente AND av.year_month = s.year_month
        LEFT JOIN fact_cliente_historico h
            ON av.cod_cliente = h.cod_cliente AND h.year_month = ?
        WHERE {where}
          AND av.year_month = ?
          AND av.venta_actual > 0
          AND (h.kg_vendidos IS NULL OR h.kg_vendidos = 0)
        ORDER BY av.venta_actual DESC
        LIMIT 5
    """, [prev_ym] + params + [cur_ym]).fetchall()

    new_buyers = [dict(r) for r in new_buyers_rows]

    conn.close()

    return jsonify({
        'mes': cur_ym,
        'forecast': forecast,
        'risk': risk,
        'opportunities': opportunities,
        'new_buyers': new_buyers,
    })


@app.route('/api/coberturas')
def api_coberturas():
    """Coverage dashboard: resumen per launch, detalle per client, rotation data."""
    cod_vendedor = request.args.get('vendedor', '')
    jefe = request.args.get('jefe', '')
    zona = request.args.get('zona', '')
    producto = request.args.get('producto', '')  # launch product filter

    conn = get_db()

    # Build where clause
    where_parts = ["year_month = (SELECT MAX(year_month) FROM fact_lanzamiento_cobertura)"]
    params = []
    if cod_vendedor:
        where_parts.append("cod_vendedor = ?")
        params.append(cod_vendedor)
    elif jefe:
        # Get vendedor codes for this jefe from avance table
        vcodes = conn.execute(
            "SELECT DISTINCT cod_vendedor FROM fact_avance_cliente_vendedor_month WHERE jefe = ?", (jefe,)
        ).fetchall()
        codes = [v['cod_vendedor'] for v in vcodes]
        if codes:
            where_parts.append(f"cod_vendedor IN ({','.join(['?']*len(codes))})")
            params.extend(codes)
    elif zona:
        where_parts.append("zona = ?")
        params.append(zona)

    where = " AND ".join(where_parts)

    # ── Reconciliation: cross-check lanzamiento estado with fact_facturacion ──
    # The launch Excel may track only selected SKUs. Any client who bought from
    # the launch product category in fact_facturacion is upgraded to COMPRADOR.
    # NOTE: 'VEGGIES' requires dim_product_classification to have categoria='VEGGIES'.
    # If not present, Veggies KG must come from the source Excel directly.
    CATEGORY_MAP = {
        'PAPAS':     'Papas',
        'EMBUTIDOS': 'Chorizos',
        'PESCADOS':  'ATUN',
        'UNTABLES':  'Untables',
        'VEGGIES':   'Veggies',
    }
    RB_LANZAMIENTOS = ['RB (Kids+Crunchies)', 'RB (Milanesitas)']

    lanz_ym = conn.execute(
        "SELECT MAX(year_month) FROM fact_lanzamiento_cobertura"
    ).fetchone()[0]
    fact_ym = conn.execute(
        "SELECT MAX(year_month) FROM fact_facturacion"
    ).fetchone()[0]

    if lanz_ym and fact_ym and lanz_ym == fact_ym:
        active_lanz = {r[0] for r in conn.execute(
            "SELECT DISTINCT lanzamiento FROM fact_lanzamiento_cobertura WHERE year_month = ?",
            (lanz_ym,)
        ).fetchall()}
        def _upgrade_lanz(cat, lanz_name):
            buyers = conn.execute("""
                SELECT f.cod_cliente, ROUND(SUM(f.cantidad), 2) as kg_real
                FROM fact_facturacion f
                JOIN dim_product_classification p ON f.cod_producto = p.cod_producto
                WHERE f.year_month = ? AND p.categoria = ? AND f.cantidad > 0
                GROUP BY f.cod_cliente
            """, (fact_ym, cat)).fetchall()
            for row in buyers:
                conn.execute("""
                    UPDATE fact_lanzamiento_cobertura
                    SET estado    = 'COMPRADOR',
                        fact_feb  = CASE WHEN fact_feb  = 0 OR fact_feb  IS NULL THEN ? ELSE fact_feb  END,
                        total_feb = CASE WHEN total_feb = 0 OR total_feb IS NULL THEN ? ELSE total_feb END
                    WHERE cod_cliente = ? AND lanzamiento = ? AND year_month = ?
                      AND (estado != 'COMPRADOR' OR fact_feb = 0 OR fact_feb IS NULL)
                """, (row['kg_real'], row['kg_real'], row['cod_cliente'], lanz_name, lanz_ym))

        for cat, lanz_name in CATEGORY_MAP.items():
            if lanz_name not in active_lanz:
                continue
            _upgrade_lanz(cat, lanz_name)
        for ln in RB_LANZAMIENTOS:
            if ln not in active_lanz:
                continue
            _upgrade_lanz('REBOZADOS', ln)
        conn.commit()

    # 1. Resumen per launch product
    resumen = conn.execute(f"""
        SELECT
            lanzamiento,
            COUNT(*) as total_clientes,
            SUM(CASE WHEN estado = 'COMPRADOR' THEN 1 ELSE 0 END) as compradores,
            SUM(CASE WHEN estado = 'SIN COMPRA' THEN 1 ELSE 0 END) as sin_compra,
            SUM(CASE WHEN estado = 'NO COMPRADOR' THEN 1 ELSE 0 END) as no_comprador,
            ROUND(100.0 * SUM(CASE WHEN estado = 'COMPRADOR' THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_cobertura,
            ROUND(SUM(fact_feb), 1) as total_fact_kg,
            ROUND(SUM(total_feb), 1) as total_total_kg,
            ROUND(SUM(promedio_u3), 1) as total_promedio
        FROM fact_lanzamiento_cobertura
        WHERE {where}
        GROUP BY lanzamiento
        ORDER BY pct_cobertura DESC
    """, params).fetchall()

    # 2. Detalle per client (optionally filtered by producto)
    det_where = where
    det_params = list(params)
    if producto:
        det_where += " AND lanzamiento = ?"
        det_params.append(producto)

    detalle = conn.execute(f"""
        SELECT lanzamiento, cod_cliente, nom_cliente, estado, 
               fact_feb as fact_current, pend_feb as pend_current, total_feb as total_current, 
               promedio_u3
        FROM fact_lanzamiento_cobertura
        WHERE {det_where}
        ORDER BY nom_cliente
    """, det_params).fetchall()

    # 3. Rotation: from fact_facturacion, get KG and $ per product family for current month
    fact_where_parts = [
        "f.year_month = (SELECT MAX(year_month) FROM fact_facturacion)"
    ]
    fact_params = []
    if cod_vendedor:
        fact_where_parts.append("f.cod_vendedor = ?")
        fact_params.append(cod_vendedor)

    fact_where = " AND ".join(fact_where_parts)
    rotacion = conn.execute(f"""
        SELECT
            COALESCE(p.categoria, 'OTROS') as categoria,
            ROUND(SUM(f.cantidad), 1) as kg,
            ROUND(SUM(f.importe), 0) as pesos,
            COUNT(DISTINCT f.cod_cliente) as clientes
        FROM fact_facturacion f
        LEFT JOIN dim_product_classification p ON f.cod_producto = p.cod_producto
        WHERE {fact_where}
        GROUP BY p.categoria
        HAVING kg > 0
        ORDER BY kg DESC
    """, fact_params).fetchall()

    conn.close()
    return jsonify({
        'resumen': [dict(r) for r in resumen],
        'detalle': [dict(d) for d in detalle],
        'rotacion': [dict(r) for r in rotacion]
    })


@app.route('/api/cliente/<cod_cliente>/mes/<year_month>')
def api_cliente_mes(cod_cliente, year_month):
    """
    Detailed breakdown for a client for a specific historical month.
    Returns: kg total, top products, category breakdown, and coverage snapshot.
    """
    conn = get_db()

    # KG and pesos for that month from fact_facturacion
    # Separate gross (positive rows) and NC (negative rows) for transparency
    month_summary = conn.execute("""
        SELECT SUM(cantidad)                                         as kg_neto,
               SUM(CASE WHEN cantidad > 0 THEN cantidad ELSE 0 END) as kg_bruto,
               SUM(CASE WHEN cantidad < 0 THEN cantidad ELSE 0 END) as kg_nc,
               SUM(importe)                                         as pesos_total,
               COUNT(DISTINCT cod_producto)                         as n_productos,
               COUNT(CASE WHEN cantidad < 0 THEN 1 END)             as n_nc_rows
        FROM fact_facturacion
        WHERE cod_cliente = ? AND year_month = ?
    """, (cod_cliente, year_month)).fetchone()

    kg_total    = month_summary['kg_neto']  or 0
    kg_bruto    = month_summary['kg_bruto'] or 0
    kg_nc       = month_summary['kg_nc']    or 0
    pesos_total = month_summary['pesos_total'] or 0
    n_prods     = month_summary['n_productos'] or 0
    has_nc      = (month_summary['n_nc_rows'] or 0) > 0
    # If no NC rows found in fact_facturacion, check if historico (net) differs from gross
    hist_row_check = conn.execute("""
        SELECT kg_vendidos FROM fact_cliente_historico
        WHERE cod_cliente = ? AND year_month = ?
    """, (cod_cliente, year_month)).fetchone()
    kg_historico_net = hist_row_check['kg_vendidos'] if hist_row_check else None
    # Warn if gross differs from net by more than 2%
    nc_missing_warning = (
        not has_nc and
        kg_historico_net is not None and
        kg_bruto > 0 and
        abs(kg_bruto - kg_historico_net) / kg_bruto > 0.02
    )

    # Top 10 products
    top_prods = conn.execute("""
        SELECT f.cod_producto,
               COALESCE(p.descripcion, f.cod_producto) as descripcion,
               COALESCE(p.categoria, 'SIN CAT') as categoria,
               SUM(f.cantidad) as kg,
               SUM(f.importe) as pesos
        FROM fact_facturacion f
        LEFT JOIN dim_product_classification p ON f.cod_producto = p.cod_producto
        WHERE f.cod_cliente = ? AND f.year_month = ?
        GROUP BY f.cod_producto
        ORDER BY kg DESC LIMIT 10
    """, (cod_cliente, year_month)).fetchall()

    # Category breakdown
    categorias = conn.execute("""
        SELECT COALESCE(p.categoria, 'SIN CATEGORIA') as categoria,
               SUM(f.cantidad) as kg,
               SUM(f.importe) as pesos
        FROM fact_facturacion f
        LEFT JOIN dim_product_classification p ON f.cod_producto = p.cod_producto
        WHERE f.cod_cliente = ? AND f.year_month = ?
        GROUP BY categoria ORDER BY kg DESC
    """, (cod_cliente, year_month)).fetchall()

    # Coverage snapshot: which launch products did they buy this month?
    # Match via product description prefix (same logic as historial-3m)
    launches = conn.execute("""
        SELECT lz.lanzamiento,
               CASE WHEN EXISTS (
                   SELECT 1 FROM fact_facturacion f
                   JOIN dim_product_classification p ON f.cod_producto = p.cod_producto
                   WHERE f.cod_cliente = ?
                     AND f.year_month = ?
                     AND (UPPER(p.descripcion) LIKE '%' || UPPER(SUBSTR(lz.lanzamiento,1,8)) || '%'
                          OR UPPER(p.categoria)  LIKE '%' || UPPER(SUBSTR(lz.lanzamiento,1,8)) || '%')
               ) THEN 'COMPRADOR' ELSE 'NO COMPRADOR' END as estado
        FROM (SELECT DISTINCT lanzamiento FROM fact_lanzamiento_cobertura) lz
        ORDER BY lz.lanzamiento
    """, (cod_cliente, year_month)).fetchall()

    # Current avance month objective for reference
    obj_row = conn.execute("""
        SELECT objetivo FROM fact_avance_cliente_vendedor_month
        WHERE cod_cliente = ? ORDER BY year_month DESC LIMIT 1
    """, (cod_cliente,)).fetchone()
    objetivo_ref = obj_row['objetivo'] if obj_row else None

    # Historical kg for this month (from historico table)
    hist_row = conn.execute("""
        SELECT kg_vendidos FROM fact_cliente_historico
        WHERE cod_cliente = ? AND year_month = ?
    """, (cod_cliente, year_month)).fetchone()
    kg_historico = hist_row['kg_vendidos'] if hist_row else kg_total

    # Check for overdue invoices (last 3 months) or 'Anticipado' condition
    alerta_anticipado = False
    facturas_vencidas = []
    
    # 1. Check if client is Anticipado
    plazo_row = conn.execute("SELECT plazo FROM dim_clients WHERE cliente_id = ?", (cod_cliente,)).fetchone()
    if plazo_row and plazo_row['plazo'] and str(plazo_row['plazo']).strip().lower() == 'anticipado':
        alerta_anticipado = True
        
    # 2. Check for overdue invoices in the last 90 days
    # (Assuming fact_facturacion imports positive rows for invoices)
    # We find rows where fecha_emision + plazo < today
    # We use julianday for date math
    deuda = conn.execute("""
        SELECT f.fecha_emision, f.importe, p.plazo
        FROM fact_facturacion f
        JOIN dim_clients p ON f.cod_cliente = p.cliente_id
        WHERE f.cod_cliente = ?
          AND f.cantidad > 0
          AND f.fecha_emision >= date('now', '-90 day')
          AND p.plazo IS NOT NULL AND LOWER(p.plazo) != 'anticipado'
          AND julianday('now') - julianday(f.fecha_emision) > CAST(p.plazo AS INTEGER)
        ORDER BY f.fecha_emision ASC
    """, (cod_cliente,)).fetchall()
    
    for d in deuda:
        facturas_vencidas.append({
            'fecha_emision': d['fecha_emision'],
            'importe': round(d['importe'], 2),
            'dias_vencida': int(conn.execute("SELECT CAST(julianday('now') - (julianday(?) + ?) AS INTEGER)", (d['fecha_emision'], d['plazo'])).fetchone()[0])
        })

    conn.close()

    mes_names = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic']
    yp, mp = year_month.split('-')
    month_label = mes_names[int(mp)-1] + ' ' + yp

    return jsonify({
        'year_month': year_month,
        'month_label': month_label,
        'kg_total': round(kg_total, 0),            # net (bruto - NCs)
        'kg_bruto': round(kg_bruto, 0),            # gross invoiced
        'kg_nc': round(kg_nc, 0),                  # NC deductions (negative value)
        'kg_historico_net': round(kg_historico_net, 0) if kg_historico_net else None,
        'has_nc': has_nc,
        'nc_missing_warning': nc_missing_warning,  # True when gross >> net (NCs not in TXT)
        'pesos_total': round(pesos_total, 0),
        'n_productos': n_prods,
        'objetivo_ref': objetivo_ref,
        'pct_objetivo': round(kg_total / objetivo_ref * 100, 1) if objetivo_ref and kg_total else None,
        'top_productos': [dict(r) for r in top_prods],
        'categorias': [dict(r) for r in categorias],
        'coberturas': [dict(r) for r in launches],
        'alerta_anticipado': alerta_anticipado,
        'facturas_vencidas': facturas_vencidas
    })


@app.route('/api/cliente/<cod_cliente>/coberturas')
def api_cliente_coberturas(cod_cliente):
    """Coverage status for a specific client across all launch products."""
    conn = get_db()
    rows = conn.execute("""
        SELECT lanzamiento, estado, fact_feb, pend_feb, total_feb, promedio_u3
        FROM fact_lanzamiento_cobertura
        WHERE cod_cliente = ?
          AND year_month = (SELECT MAX(year_month) FROM fact_lanzamiento_cobertura)
        ORDER BY lanzamiento
    """, (cod_cliente,)).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/coberturas/evolucion-lanzamientos')
def api_coberturas_evolucion_lanzamientos():
    """
    Evolución histórica de KG vendidos por lanzamiento (categoría) a lo largo del tiempo.
    Agrupa fact_facturacion × dim_product_classification por year_month y categoría.
    Opcionalmente filtra por cliente, vendedor, jefe o zona.
    """
    cod_vendedor = request.args.get('vendedor', '')
    jefe = request.args.get('jefe', '')
    zona = request.args.get('zona', '')
    cod_cliente = request.args.get('cod_cliente', '')
    meses = int(request.args.get('meses', 6))

    conn = get_db()

    # Obtener los últimos N meses disponibles
    meses_rows = conn.execute("""
        SELECT DISTINCT year_month FROM fact_facturacion
        ORDER BY year_month DESC LIMIT ?
    """, (meses,)).fetchall()

    if not meses_rows:
        conn.close()
        return jsonify({'error': 'No hay datos de facturación'}), 404

    month_list = sorted([r['year_month'] for r in meses_rows])
    cutoff = month_list[0]

    # Obtener los lanzamientos (categorías) disponibles en fact_lanzamiento_cobertura
    # para saber cuáles son los productos de lanzamiento
    lanz_rows = conn.execute("""
        SELECT DISTINCT lanzamiento FROM fact_lanzamiento_cobertura
        WHERE year_month = (SELECT MAX(year_month) FROM fact_lanzamiento_cobertura)
        ORDER BY lanzamiento
    """).fetchall()
    lanzamientos = [r['lanzamiento'] for r in lanz_rows]

    # Build where clause for fact_facturacion
    where_parts = ["f.year_month >= ?"]
    params = [cutoff]

    if cod_cliente:
        where_parts.append("f.cod_cliente = ?")
        params.append(cod_cliente)
    elif cod_vendedor:
        where_parts.append("f.cod_vendedor = ?")
        params.append(cod_vendedor)
    elif jefe:
        vcodes = conn.execute(
            "SELECT DISTINCT cod_vendedor FROM fact_avance_cliente_vendedor_month WHERE jefe = ?", (jefe,)
        ).fetchall()
        codes = [v['cod_vendedor'] for v in vcodes]
        if codes:
            where_parts.append(f"f.cod_vendedor IN ({','.join(['?']*len(codes))})")
            params.extend(codes)
    elif zona:
        where_parts.append("f.zona = ?" if False else "1=1")  # zona not in fact_facturacion directly
        vcodes = conn.execute(
            "SELECT DISTINCT cod_vendedor FROM fact_avance_cliente_vendedor_month WHERE zona = ?", (zona,)
        ).fetchall()
        codes = [v['cod_vendedor'] for v in vcodes]
        if codes:
            where_parts[-1] = f"f.cod_vendedor IN ({','.join(['?']*len(codes))})"
            params.extend(codes)

    where = " AND ".join(where_parts)

    # Query: KG por (year_month × categoria) — todas las categorías con datos
    rows = conn.execute(f"""
        SELECT
            f.year_month,
            COALESCE(p.categoria, 'OTROS') as lanzamiento,
            ROUND(SUM(f.cantidad), 1) as kg
        FROM fact_facturacion f
        LEFT JOIN dim_product_classification p ON f.cod_producto = p.cod_producto
        WHERE {where}
          AND f.cantidad > 0
        GROUP BY f.year_month, p.categoria
        ORDER BY f.year_month ASC, kg DESC
    """, params).fetchall()

    conn.close()

    # Pivot: {year_month: {lanzamiento: kg}}
    pivot = {}
    all_lanz = []
    seen_lanz = set()
    for r in rows:
        ym = r['year_month']
        lz = r['lanzamiento']
        if ym not in pivot:
            pivot[ym] = {}
        pivot[ym][lz] = r['kg']
        if lz not in seen_lanz:
            all_lanz.append(lz)
            seen_lanz.add(lz)

    # Use lanzamientos order from cobertura (if available), else from data
    ordered_lanz = [l for l in lanzamientos if l in seen_lanz]
    # Add any extra categories from data not in lanzamientos list
    ordered_lanz += [l for l in all_lanz if l not in set(ordered_lanz)]

    # Build series per lanzamiento
    series = {}
    for lz in ordered_lanz:
        series[lz] = [pivot.get(ym, {}).get(lz, 0) for ym in month_list]

    # Totales por mes
    totales_mes = [round(sum(pivot.get(ym, {}).values()), 1) for ym in month_list]

    # Etiquetas de meses
    MESES_ES = {
        '01': 'Ene', '02': 'Feb', '03': 'Mar', '04': 'Abr',
        '05': 'May', '06': 'Jun', '07': 'Jul', '08': 'Ago',
        '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dic'
    }
    mes_labels = []
    for ym in month_list:
        parts = ym.split('-')
        mes_labels.append(f"{MESES_ES.get(parts[1], parts[1])} {parts[0][2:]}" if len(parts) == 2 else ym)

    return jsonify({
        'meses': month_list,
        'mes_labels': mes_labels,
        'lanzamientos': ordered_lanz,
        'series': series,
        'totales_mes': totales_mes,
        'filtrado_por_cliente': bool(cod_cliente)
    })


@app.route('/api/coberturas/cliente-evolucion')
def api_cliente_evolucion():
    """
    Evolución mensual del mix de categorías compradas por un cliente.
    Devuelve series por categoría × mes (KG) para los últimos N meses disponibles.
    """
    cod_cliente = request.args.get('cod_cliente', '')
    meses = int(request.args.get('meses', 6))

    if not cod_cliente:
        return jsonify({'error': 'cod_cliente requerido'}), 400

    conn = get_db()

    # Obtener nombre del cliente
    cliente_row = conn.execute("""
        SELECT DISTINCT nom_cliente FROM fact_avance_cliente_vendedor_month
        WHERE cod_cliente = ? LIMIT 1
    """, (cod_cliente,)).fetchone()

    nombre_cliente = cliente_row['nom_cliente'] if cliente_row else cod_cliente

    # Obtener los últimos N meses disponibles en fact_facturacion
    meses_disponibles = conn.execute("""
        SELECT DISTINCT year_month FROM fact_facturacion
        ORDER BY year_month DESC LIMIT ?
    """, (meses,)).fetchall()

    if not meses_disponibles:
        conn.close()
        return jsonify({'error': 'No hay datos de facturación'}), 404

    month_list = sorted([r['year_month'] for r in meses_disponibles])
    cutoff = month_list[0]

    # Obtener KG por mes y categoría para el cliente
    rows = conn.execute("""
        SELECT
            f.year_month,
            COALESCE(p.categoria, 'OTROS') as categoria,
            ROUND(SUM(f.cantidad), 1) as kg,
            ROUND(SUM(f.importe), 0) as pesos,
            COUNT(DISTINCT f.cod_producto) as productos
        FROM fact_facturacion f
        LEFT JOIN dim_product_classification p ON f.cod_producto = p.cod_producto
        WHERE f.cod_cliente = ?
          AND f.year_month >= ?
        GROUP BY f.year_month, p.categoria
        ORDER BY f.year_month ASC, kg DESC
    """, (cod_cliente, cutoff)).fetchall()

    conn.close()

    # Construir estructura de series: {categoria: [kg_mes1, kg_mes2, ...]}
    cat_set = []
    seen_cats = set()
    for r in rows:
        if r['categoria'] not in seen_cats:
            cat_set.append(r['categoria'])
            seen_cats.add(r['categoria'])

    # Pivot: mes × categoria → kg
    pivot = {}  # {year_month: {categoria: kg}}
    for r in rows:
        ym = r['year_month']
        if ym not in pivot:
            pivot[ym] = {}
        pivot[ym][r['categoria']] = r['kg']

    # Build series arrays aligned to month_list
    series = {}
    for cat in cat_set:
        series[cat] = [pivot.get(ym, {}).get(cat, 0) for ym in month_list]

    # Totales por mes
    totales_mes = []
    for ym in month_list:
        totales_mes.append(round(sum(pivot.get(ym, {}).values()), 1))

    # Formato de etiquetas de meses (ej: "Nov 25", "Dic 25")
    mes_labels = []
    MESES_ES = {
        '01': 'Ene', '02': 'Feb', '03': 'Mar', '04': 'Abr',
        '05': 'May', '06': 'Jun', '07': 'Jul', '08': 'Ago',
        '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dic'
    }
    for ym in month_list:
        parts = ym.split('-')
        if len(parts) == 2:
            mes_labels.append(f"{MESES_ES.get(parts[1], parts[1])} {parts[0][2:]}")
        else:
            mes_labels.append(ym)

    return jsonify({
        'cliente': nombre_cliente,
        'cod_cliente': cod_cliente,
        'meses': month_list,
        'mes_labels': mes_labels,
        'categorias': cat_set,
        'series': series,
        'totales_mes': totales_mes
    })


@app.route('/api/coberturas/historial-3m')
def api_coberturas_historial_3m():
    """
    Compares coverage (unique buyers) across 10 defined categories (Lanzamientos/Focos)
    across the last 3 billing months using fact_facturacion + dim_product_classification.
    Special rule for UNTABLES: only counts if product description contains JALAPEÑO, JAPALEÑO or CHILE.
    """
    cod_vendedor = request.args.get('vendedor', '')
    jefe         = request.args.get('jefe', '')
    zona         = request.args.get('zona', '')

    if not any([cod_vendedor, jefe, zona]):
        return jsonify({'error': 'filter required'}), 400

    conn = get_db()

    # Build vendor filter
    if cod_vendedor:
        where_av = "av.cod_vendedor = ?"
        av_params = [cod_vendedor]
    elif jefe:
        where_av = "av.jefe = ?"
        av_params = [jefe]
    else:
        where_av = "av.zona = ?"
        av_params = [zona]

    # Resolve vendor codes for fact_facturacion filter
    cur_ym_row = conn.execute("SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month").fetchone()
    if not cur_ym_row or not cur_ym_row[0]:
        conn.close()
        return jsonify({'meses': [], 'lanzamientos': [], 'series': {}})
    cur_ym = cur_ym_row[0]
    
    vendor_codes_rows = conn.execute(f"""
        SELECT DISTINCT av.cod_vendedor FROM fact_avance_cliente_vendedor_month av
        WHERE {where_av} AND av.year_month = ?
    """, av_params + [cur_ym]).fetchall()
    vendor_codes = [r['cod_vendedor'] for r in vendor_codes_rows]

    if not vendor_codes:
        conn.close()
        return jsonify({'meses': [], 'lanzamientos': [], 'series': {}})

    # Total clients in scope (denominator for coverage %)
    total_clients = conn.execute(f"""
        SELECT COUNT(DISTINCT av.cod_cliente) FROM fact_avance_cliente_vendedor_month av
        WHERE {where_av} AND av.year_month = ?
    """, av_params + [cur_ym]).fetchone()[0] or 1

    ph = ','.join(['?'] * len(vendor_codes))

    # Get last 3 billing months available
    months_rows = conn.execute(f"""
        SELECT DISTINCT year_month FROM fact_facturacion
        WHERE cod_vendedor IN ({ph})
        ORDER BY year_month DESC LIMIT 3
    """, vendor_codes).fetchall()
    months = sorted([r['year_month'] for r in months_rows])

    if not months:
        conn.close()
        return jsonify({'meses': [], 'lanzamientos': [], 'series': {}})

    month_ph = ','.join(['?'] * len(months))

    # Dynamic rules for classifying 'lanzamiento' groups using CASE WHEN
    # Re-maps categories or description patterns to the predefined 10 focus goals
    sql_classification = """
        CASE 
            WHEN UPPER(p.categoria) LIKE '%HAMBURGUESA%' THEN 'HG'
            WHEN UPPER(p.categoria) LIKE '%SALCHICHA%' THEN 'SCH'
            WHEN UPPER(p.categoria) LIKE '%REBOZADO%' OR UPPER(p.descripcion) LIKE '%REBOZADO%' THEN 'RB'
            WHEN UPPER(p.descripcion) LIKE '%SOJA%' OR UPPER(p.categoria) LIKE '%SOJA%' THEN 'SJ'
            WHEN UPPER(p.descripcion) LIKE '%GRASA%' THEN 'GRASA'
            WHEN UPPER(p.descripcion) LIKE '%PICADA%' THEN 'CARNE PICADA'
            WHEN UPPER(p.categoria) LIKE '%PAPA%' THEN 'PAPAS'
            WHEN UPPER(p.categoria) = 'PESCADOS' OR UPPER(p.descripcion) LIKE '%ATUN%' THEN 'ATUN'
            WHEN UPPER(p.categoria) = 'EMBUTIDOS' OR UPPER(p.descripcion) LIKE '%CHORIZO%' THEN 'CHORIZOS'
            WHEN UPPER(p.categoria) = 'UNTABLES' 
                 AND (UPPER(p.descripcion) LIKE '%JALAPEÑO%' OR UPPER(p.descripcion) LIKE '%JAPALEÑO%' OR UPPER(p.descripcion) LIKE '%CHILE%') THEN 'UNT'
            ELSE NULL
        END
    """

    # Get buyers per launch category per month directly from facts
    rows = conn.execute(f"""
        SELECT 
            f.year_month,
            {sql_classification} as lanzamiento,
            COUNT(DISTINCT f.cod_cliente) as compradores,
            SUM(f.cantidad) as kg_total
        FROM fact_facturacion f
        JOIN dim_product_classification p ON f.cod_producto = p.cod_producto
        WHERE f.cod_vendedor IN ({ph})
          AND f.year_month IN ({month_ph})
          AND {sql_classification} IS NOT NULL
        GROUP BY f.year_month, lanzamiento
        ORDER BY lanzamiento, f.year_month
    """, vendor_codes + months).fetchall()

    # Build series structure
    series = {}   # launch → {month → {compradores, kg, pct}}
    for r in rows:
        lz = r['lanzamiento']
        ym = r['year_month']
        if lz not in series:
            series[lz] = {}
        series[lz][ym] = {
            'compradores': r['compradores'],
            'kg': round(r['kg_total'] or 0, 0),
            'pct': round(r['compradores'] / total_clients * 100, 1)
        }

    # Ensure all 10 target categories exist in the series dictionary with zeros if no sales
    target_categories = ['HG', 'SCH', 'UNT', 'RB', 'SJ', 'GRASA', 'CARNE PICADA', 'PAPAS', 'ATUN', 'CHORIZOS']
    for lz in target_categories:
        if lz not in series:
            series[lz] = {}
        for m in months:
            if m not in series[lz]:
                series[lz][m] = {'compradores': 0, 'kg': 0, 'pct': 0}

    # Compute trend: last month vs first month
    for lz in series:
        if len(months) >= 2:
            first = series[lz][months[0]]['compradores']
            last  = series[lz][months[-1]]['compradores']
            series[lz]['_trend'] = round((last - first) / first * 100, 1) if first > 0 else (100 if last > 0 else 0)
        else:
            series[lz]['_trend'] = 0

    conn.close()

    # Month labels
    mes_names = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic']
    mes_labels = [mes_names[int(m.split('-')[1]) - 1] + ' ' + m.split('-')[0][2:] for m in months]

    # Return predefined order
    return jsonify({
        'meses': months,
        'mes_labels': mes_labels,
        'total_clientes': total_clients,
        'lanzamientos': target_categories,
        'series': series
    })


@app.route('/api/forecast')
def api_forecast():
    """
    Multi-factor sales forecast per client.
    Algorithm: BASELINE × TREND × SEASONALITY × RECENCY
    - BASELINE: trimmed 6-month mean (drop outliers)
    - TREND: linear slope of last 3 months, capped ±30%
    - SEASONALITY: ratio of same-month-prior-years vs annual mean
    - RECENCY: penalises clients who missed recent months (churn signal)
    Confidence interval: ±1σ of monthly distribution (≈68%)
    """
    import statistics

    cod_vendedor = request.args.get('vendedor', '')
    jefe         = request.args.get('jefe', '')
    zona         = request.args.get('zona', '')

    if not any([cod_vendedor, jefe, zona]):
        return jsonify({'error': 'filter required'}), 400

    conn = get_db()

    if cod_vendedor:
        where_av = "av.cod_vendedor = ?"
        av_params = [cod_vendedor]
    elif jefe:
        where_av = "av.jefe = ?"
        av_params = [jefe]
    else:
        where_av = "av.zona = ?"
        av_params = [zona]

    cur_ym = conn.execute(
        "SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month"
    ).fetchone()[0]
    y_cur, m_cur = map(int, cur_ym.split('-'))
    next_m = m_cur + 1 if m_cur < 12 else 1
    next_y = y_cur if m_cur < 12 else y_cur + 1
    next_ym = f"{next_y}-{next_m:02d}"

    # Clients in scope
    clients = conn.execute(f"""
        SELECT av.cod_cliente, av.nom_cliente, av.objetivo, av.venta_actual,
               s.tier, s.score
        FROM fact_avance_cliente_vendedor_month av
        LEFT JOIN fact_client_segmentation s
            ON av.cod_cliente = s.cod_cliente AND av.year_month = s.year_month
        WHERE {where_av} AND av.year_month = ?
    """, av_params + [cur_ym]).fetchall()

    # Zone-level monthly average for benchmark
    zone_monthly_avgs = conn.execute(f"""
        SELECT h.year_month, AVG(h.kg_vendidos) as avg_kg
        FROM fact_cliente_historico h
        JOIN fact_avance_cliente_vendedor_month av
            ON h.cod_cliente = av.cod_cliente
        WHERE {where_av} AND av.year_month = ?
          AND h.kg_vendidos > 0
        GROUP BY h.year_month
        ORDER BY h.year_month
    """, av_params + [cur_ym]).fetchall()
    zone_avg_map = {r['year_month']: r['avg_kg'] for r in zone_monthly_avgs}

    # Launch engagement count per client (active in lanzamientos this month)
    launch_counts = conn.execute(f"""
        SELECT lz.cod_cliente, COUNT(DISTINCT lz.lanzamiento) as n_launches
        FROM fact_lanzamiento_cobertura lz
        JOIN fact_avance_cliente_vendedor_month av
            ON lz.cod_cliente = av.cod_cliente
        WHERE {where_av} AND av.year_month = ?
          AND lz.year_month = ?
          AND lz.estado = 'COMPRADOR'
        GROUP BY lz.cod_cliente
    """, av_params + [cur_ym, cur_ym]).fetchall()
    launch_map = {r['cod_cliente']: r['n_launches'] for r in launch_counts}
    max_launches = max(launch_map.values(), default=1)

    forecasts = []
    for client in clients:
        cid = client['cod_cliente']

        # Full history from fact_cliente_historico (sorted asc)
        hist_rows = conn.execute("""
            SELECT year_month, kg_vendidos FROM fact_cliente_historico
            WHERE cod_cliente = ? AND kg_vendidos > 0
            ORDER BY year_month ASC
        """, (cid,)).fetchall()
        hist = [(r['year_month'], r['kg_vendidos']) for r in hist_rows]

        if not hist:
            # No history: use current venta_actual as single data point
            if client['venta_actual'] and client['venta_actual'] > 0:
                fc = round(client['venta_actual'], 0)
                forecasts.append({
                    'cod_cliente': cid,
                    'nom_cliente': client['nom_cliente'],
                    'tier': client['tier'],
                    'forecast_kg': fc,
                    'low_kg': round(fc * 0.75, 0),
                    'high_kg': round(fc * 1.25, 0),
                    'confidence': 30,
                    'factors': {'note': 'sin historial — estimado desde mes actual'},
                    'objetivo_kg': client['objetivo'],
                    'venta_actual': client['venta_actual'],
                })
            continue

        vals = [kg for _, kg in hist]
        months_list = [ym for ym, _ in hist]

        # ── 1. BASELINE: trimmed 6M mean ──────────────────────────
        recent_6 = vals[-6:]
        if len(recent_6) >= 4:
            s6 = sorted(recent_6)
            baseline = statistics.mean(s6[1:-1])   # drop min and max
        else:
            baseline = statistics.mean(recent_6)

        if baseline <= 0:
            continue

        # ── 2. TREND: linear slope on last 3 months ───────────────
        r3 = vals[-3:]
        n3 = len(r3)
        if n3 >= 2:
            x = list(range(n3))
            xm, ym_ = statistics.mean(x), statistics.mean(r3)
            denom = sum((xi - xm)**2 for xi in x)
            slope = sum((xi-xm)*(yi-ym_) for xi, yi in zip(x,r3)) / denom if denom else 0
            trend_factor = 1.0 + max(-0.30, min(0.30, slope / baseline))
        else:
            trend_factor = 1.0

        # ── 3. SEASONALITY: same month in prior data vs overall mean ─
        target_m_num = next_m
        same_m_vals = [kg for ym_, kg in hist if int(ym_.split('-')[1]) == target_m_num and kg > 0]
        all_mean = statistics.mean(vals) if vals else baseline
        if same_m_vals and all_mean > 0:
            season_factor = statistics.mean(same_m_vals) / all_mean
            season_factor = max(0.65, min(1.50, season_factor))
        else:
            season_factor = 1.0

        # ── 4. RECENCY: penalise missed recent months ──────────────
        last_2 = vals[-2:]
        zeros_recent = sum(1 for v in last_2 if v == 0)
        recency_factor = 1.0 if zeros_recent == 0 else (0.80 if zeros_recent == 1 else 0.60)

        # ── 5. LAUNCH engagement bonus ─────────────────────────────
        n_lz = launch_map.get(cid, 0)
        launch_factor = 1.0 + 0.08 * (n_lz / max(max_launches, 1))  # up to +8% for top buyer

        # ── 6. ZONE benchmark ──────────────────────────────────────
        # client's last 3M avg relative to zone avg → small corrective weight
        zone_avg_3m = statistics.mean([
            zone_avg_map.get(m, 0) for m in months_list[-3:]
        ]) if zone_avg_map else 0
        client_avg_3m = statistics.mean(vals[-3:]) if vals else 0
        if zone_avg_3m > 0:
            zone_ratio = client_avg_3m / zone_avg_3m
            zone_factor = 1.0 + 0.05 * max(-1, min(1, zone_ratio - 1))  # ±5% weight
        else:
            zone_factor = 1.0

        # ── Combined forecast ──────────────────────────────────────
        forecast_kg = baseline * trend_factor * season_factor * recency_factor * launch_factor * zone_factor
        forecast_kg = max(0, round(forecast_kg, 0))

        # ── Confidence interval: ±1σ of historical distribution ───
        if len(vals) >= 2:
            std = statistics.stdev(vals)
        else:
            std = baseline * 0.20
        low_kg  = max(0, round(forecast_kg - std, 0))
        high_kg = round(forecast_kg + std, 0)

        # Confidence: grows with history length (30% for 1M, 85% for 12M+)
        confidence = min(85, 30 + 5 * len(vals))

        forecasts.append({
            'cod_cliente': cid,
            'nom_cliente': client['nom_cliente'],
            'tier': client['tier'],
            'forecast_kg': forecast_kg,
            'low_kg': low_kg,
            'high_kg': high_kg,
            'confidence': confidence,
            'objetivo_kg': client['objetivo'],
            'venta_actual': client['venta_actual'],
            'hist_months': len(vals),
            'factors': {
                'baseline_kg': round(baseline, 0),
                'trend': round(trend_factor, 3),
                'seasonality': round(season_factor, 3),
                'recency': recency_factor,
                'launch_engagement': round(launch_factor, 3),
                'zone_benchmark': round(zone_factor, 3),
            }
        })

    conn.close()

    # Sort by forecast desc
    forecasts.sort(key=lambda x: x['forecast_kg'], reverse=True)

    # Aggregate forecast for header
    total_forecast = sum(f['forecast_kg'] for f in forecasts)
    total_low = sum(f['low_kg'] for f in forecasts)
    total_high = sum(f['high_kg'] for f in forecasts)

    # Objective for next month (same as current, no new file yet)
    total_obj = conn2 = None
    try:
        conn2 = get_db()
        obj_row = conn2.execute(f"""
            SELECT SUM(objetivo) as obj FROM fact_avance_cliente_vendedor_month
            WHERE {where_av} AND year_month = ?
        """, av_params + [cur_ym]).fetchone()
        total_obj = round(obj_row['obj'] or 0, 0)
        conn2.close()
    except Exception:
        if conn2: conn2.close()

    return jsonify({
        'next_month': next_ym,
        'current_month': cur_ym,
        'total_forecast_kg': round(total_forecast, 0),
        'total_low_kg': round(total_low, 0),
        'total_high_kg': round(total_high, 0),
        'total_objetivo_kg': total_obj,
        'proj_pct_of_obj': round(total_forecast / total_obj * 100, 1) if total_obj else None,
        'clients': forecasts
    })

def _build_deuda_alertas(conn, clientes, dia_string, fecha_visita, tipo_suffix):
    """Build debt/contado anticipado alerts for a list of clients. Returns list with alert_id."""
    alertas = []
    for cl in clientes:
        cid = cl['cod_cliente']
        plazo = str(cl['plazo']).strip().lower() if cl['plazo'] else ''
        es_anticipado = (plazo == 'anticipado')

        deuda = []
        if not es_anticipado and plazo and plazo.isdigit():
            vencidas_rows = conn.execute("""
                SELECT fecha_emision, importe
                FROM fact_facturacion
                WHERE cod_cliente = ?
                  AND cantidad > 0
                  AND fecha_emision >= date('now', '-90 day')
                  AND julianday('now') - julianday(fecha_emision) > ?
                ORDER BY fecha_emision ASC
            """, (cid, int(plazo))).fetchall()
            for d in vencidas_rows:
                dias_v = int(conn.execute(
                    "SELECT CAST(julianday('now') - (julianday(?) + ?) AS INTEGER)",
                    (d['fecha_emision'], int(plazo))
                ).fetchone()[0])
                deuda.append({
                    'fecha_emision': d['fecha_emision'],
                    'importe': round(d['importe'], 2),
                    'dias_vencida': dias_v
                })

        if es_anticipado or len(deuda) > 0:
            sub = 'CONTADO' if es_anticipado else 'DEUDA'
            alert_id = f"{sub}_{cid}_{fecha_visita}_{tipo_suffix}"
            alertas.append({
                'alert_id': alert_id,
                'cod_cliente': cid,
                'nom_cliente': cl['nom_cliente'],
                'es_anticipado': es_anticipado,
                'plazo_original': cl['plazo'],
                'cantidad_facturas_vencidas': len(deuda),
                'total_vencido': sum(d['importe'] for d in deuda),
                'facturas_vencidas': deuda,
                'tipo': 'CONTADO_ANTICIPADO' if es_anticipado else 'DEUDA',
                'dia_visita': dia_string,
                'fecha_visita': fecha_visita,
            })
    return alertas


@app.route('/api/alertas/deuda-manana')
def api_alertas_deuda_manana():
    """
    Returns a list of clients who are due for visit TOMORROW
    and have either: 'Contado anticipado' or Overdue invoices.
    Query param `date` (YYYY-MM-DD): use that as "today", so mañana = date + 1.
    """
    from datetime import datetime, timedelta
    date_str = request.args.get('date')
    if date_str:
        try:
            hoy_ref = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            hoy_ref = datetime.now()
    else:
        hoy_ref = datetime.now()
    mañana = hoy_ref + timedelta(days=1)
    weekday_mañana = mañana.weekday()
    mapping_dias = {0: 'LUNES', 1: 'MARTES', 2: 'MIERCOLES', 3: 'JUEVES',
                   4: 'VIERNES', 5: 'SABADO', 6: 'DOMINGO'}
    dia_string = mapping_dias.get(weekday_mañana)
    fecha_visita = mañana.strftime('%Y-%m-%d')

    conn = get_db()
    where_parts, params = _alertas_where_clause(request)
    clientes = conn.execute(f"""
        SELECT av.cod_cliente, av.nom_cliente, c.plazo, av.frecuencia
        FROM fact_avance_cliente_vendedor_month av
        JOIN dim_clients c ON av.cod_cliente = c.cliente_id
        WHERE {where_parts} AND UPPER(av.frecuencia) LIKE ?
    """, params + [f"%{dia_string}%"]).fetchall()

    alertas = _build_deuda_alertas(conn, clientes, dia_string, fecha_visita, 'MANANA')
    alertas = _filter_dismissed(conn, alertas)
    conn.close()

    alertas.sort(key=lambda x: x['total_vencido'], reverse=True)
    return jsonify({
        'dia_cobro': dia_string,
        'fecha_cobro': fecha_visita,
        'total_alertas': len(alertas),
        'alertas': alertas
    })


def _alertas_where_clause(req):
    where_parts = ["av.year_month = (SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month)"]
    params = []
    v, j, z = req.args.get('vendedor', ''), req.args.get('jefe', ''), req.args.get('zona', '')
    if v:
        where_parts.append("av.cod_vendedor = ?")
        params.append(v)
    elif j:
        where_parts.append("av.jefe = ?")
        params.append(j)
    elif z:
        where_parts.append("av.zona = ?")
        params.append(z)
    return " AND ".join(where_parts), params


def _filter_dismissed(conn, alertas):
    if not alertas:
        return alertas
    ids = [a['alert_id'] for a in alertas]
    placeholders = ','.join(['?'] * len(ids))
    dismissed = set(r[0] for r in conn.execute(
        f"SELECT alert_id FROM crm_alertas_dismissed WHERE alert_id IN ({placeholders})", ids
    ).fetchall())
    return [a for a in alertas if a['alert_id'] not in dismissed]


@app.route('/api/alertas/deuda-hoy')
def api_alertas_deuda_hoy():
    """
    Returns clients due for visit TODAY with debt or contado anticipado.
    For early-day management: see who needs attention before starting the route.
    """
    from datetime import datetime
    hoy = datetime.now()
    weekday = hoy.weekday()
    mapping_dias = {0: 'LUNES', 1: 'MARTES', 2: 'MIERCOLES', 3: 'JUEVES',
                    4: 'VIERNES', 5: 'SABADO', 6: 'DOMINGO'}
    # Planning mapping: Mon->Tue,Wed; Tue->Wed,Thu; Wed->Thu,Fri; Thu->Fri,Mon; Fri->Mon,Tue
    freq_map = {0: ['MARTES', 'MIERCOLES'], 1: ['MIERCOLES', 'JUEVES'], 2: ['JUEVES', 'VIERNES'],
                3: ['VIERNES', 'LUNES'], 4: ['LUNES', 'MARTES'], 5: [], 6: []}
    target_freqs = freq_map.get(weekday, [])
    fecha_visita = hoy.strftime('%Y-%m-%d')
    dia_string = mapping_dias.get(weekday, '')

    conn = get_db()
    where_parts, params = _alertas_where_clause(request)
    if not target_freqs:
        conn.close()
        return jsonify({'dia_visita': dia_string, 'fecha_visita': fecha_visita, 'total_alertas': 0, 'alertas': []})

    freq_cond = " OR ".join(["UPPER(av.frecuencia) LIKE ?" for _ in target_freqs])
    params_freq = params + [f"%{f}%" for f in target_freqs]
    clientes = conn.execute(f"""
        SELECT av.cod_cliente, av.nom_cliente, c.plazo, av.frecuencia
        FROM fact_avance_cliente_vendedor_month av
        JOIN dim_clients c ON av.cod_cliente = c.cliente_id
        WHERE {where_parts} AND ({freq_cond})
    """, params_freq).fetchall()

    alertas = _build_deuda_alertas(conn, clientes, dia_string or 'HOY', fecha_visita, 'HOY')
    alertas = _filter_dismissed(conn, alertas)
    conn.close()

    alertas.sort(key=lambda x: x['total_vencido'], reverse=True)
    return jsonify({
        'dia_visita': dia_string,
        'fecha_visita': fecha_visita,
        'total_alertas': len(alertas),
        'alertas': alertas
    })


@app.route('/api/alertas/gestion-completo')
def api_alertas_gestion_completo():
    """
    Unified alerts for CRM Hoy tab: deuda hoy, contado anticipado hoy, deuda mañana, gestiones.
    All with alert_id for dismiss. Filtered by vendedor/jefe/zona.
    Query param `date` (YYYY-MM-DD): simulate that date for planning (hoy/mañana relative to it).
    """
    conn = get_db()
    from datetime import datetime, timedelta

    date_str = request.args.get('date')
    if date_str:
        try:
            hoy = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            hoy = datetime.now()
    else:
        hoy = datetime.now()
    mañana = hoy + timedelta(days=1)
    mapping_dias = {0: 'LUNES', 1: 'MARTES', 2: 'MIERCOLES', 3: 'JUEVES',
                    4: 'VIERNES', 5: 'SABADO', 6: 'DOMINGO'}
    freq_map = {0: ['MARTES', 'MIERCOLES'], 1: ['MIERCOLES', 'JUEVES'], 2: ['JUEVES', 'VIERNES'],
                3: ['VIERNES', 'LUNES'], 4: ['LUNES', 'MARTES'], 5: [], 6: []}

    where_parts, params = _alertas_where_clause(request)
    all_alertas = []

    # 1. Deuda / Contado HOY (clientes que visitamos hoy)
    target_freqs = freq_map.get(hoy.weekday(), [])
    if target_freqs:
        freq_cond = " OR ".join(["UPPER(av.frecuencia) LIKE ?" for _ in target_freqs])
        params_hoy = params + [f"%{f}%" for f in target_freqs]
        clientes_hoy = conn.execute(f"""
            SELECT av.cod_cliente, av.nom_cliente, c.plazo, av.frecuencia
            FROM fact_avance_cliente_vendedor_month av
            JOIN dim_clients c ON av.cod_cliente = c.cliente_id
            WHERE {where_parts} AND ({freq_cond})
        """, params_hoy).fetchall()
        ah = _build_deuda_alertas(conn, clientes_hoy, mapping_dias.get(hoy.weekday(), 'HOY'),
                                  hoy.strftime('%Y-%m-%d'), 'HOY')
        for a in ah:
            a['seccion'] = 'deuda_hoy'
        all_alertas.extend(ah)

    # 2. Deuda / Contado MAÑANA
    dia_manana = mapping_dias.get(mañana.weekday())
    clientes_manana = conn.execute(f"""
        SELECT av.cod_cliente, av.nom_cliente, c.plazo, av.frecuencia
        FROM fact_avance_cliente_vendedor_month av
        JOIN dim_clients c ON av.cod_cliente = c.cliente_id
        WHERE {where_parts} AND UPPER(av.frecuencia) LIKE ?
    """, params + [f"%{dia_manana}%"]).fetchall()
    am = _build_deuda_alertas(conn, clientes_manana, dia_manana or 'MAÑANA',
                             mañana.strftime('%Y-%m-%d'), 'MANANA')
    for a in am:
        a['seccion'] = 'deuda_manana'
    all_alertas.extend(am)

    # 3. Gestiones (próximo paso, cold, desavance) — use simulate date when provided
    hoy_sql = hoy.strftime('%Y-%m-%d')

    gt = conn.execute("""
        SELECT g.id, 'GESTION' as tipo, 'Próximo Paso: ' || proximo_paso as descripcion,
               proximo_paso_fecha as fecha_vencimiento, 'ALTA' as prioridad, g.cod_cliente,
               (SELECT cliente_name FROM dim_clients WHERE cliente_id = g.cod_cliente) as nom_cliente
        FROM crm_gestiones g
        WHERE proximo_paso_fecha <= date(?, '+2 days') AND proximo_paso_fecha IS NOT NULL
        ORDER BY proximo_paso_fecha ASC LIMIT 10
    """, (hoy_sql,)).fetchall()
    for r in gt:
        d = dict(r)
        d['alert_id'] = f"GESTION_{d['cod_cliente']}_{d.get('fecha_vencimiento','')}_{d.get('id','')}"
        d['seccion'] = 'gestiones'
        all_alertas.append(d)

    cold = conn.execute("""
        SELECT 'FALTA_GESTION' as tipo, 'Sin gestión hace > 20 días' as descripcion,
               MAX(fecha) as fecha_vencimiento, 'MEDIA' as prioridad, cod_cliente,
               (SELECT cliente_name FROM dim_clients WHERE cliente_id = g.cod_cliente) as nom_cliente
        FROM crm_gestiones g
        GROUP BY cod_cliente
        HAVING fecha_vencimiento < date(?, '-20 days') LIMIT 5
    """, (hoy_sql,)).fetchall()
    for r in cold:
        d = dict(r)
        d['alert_id'] = f"FALTA_GESTION_{d['cod_cliente']}_{d.get('fecha_vencimiento','')}"
        d['seccion'] = 'gestiones'
        all_alertas.append(d)

    gap = conn.execute("""
        SELECT 'DESAVANCE' as tipo,
               'Bajo cumplimiento: ' || CAST(ROUND((venta_actual/objetivo)*100) AS INTEGER) || '%' as descripcion,
               date('now') as fecha_vencimiento, 'BAJA' as prioridad, cod_cliente, nom_cliente
        FROM fact_avance_cliente_vendedor_month
        WHERE objetivo > 0 AND (venta_actual/objetivo) < 0.4
          AND year_month = (SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month)
        LIMIT 5
    """).fetchall()
    for r in gap:
        d = dict(r)
        d['alert_id'] = f"DESAVANCE_{d['cod_cliente']}_{d.get('fecha_vencimiento','')}"
        d['seccion'] = 'gestiones'
        all_alertas.append(d)

    # 4. Gestiones recurrentes para la fecha (pendientes, no completadas)
    recurr_rows = conn.execute("""
        SELECT r.id, r.cod_cliente, r.descripcion, dc.cliente_name as nom_cliente
        FROM crm_planificacion_recurrente r
        LEFT JOIN dim_clients dc ON r.cod_cliente = dc.cliente_id
        LEFT JOIN crm_planificacion_recurrente_completado c ON c.recurrente_id = r.id AND c.fecha = ?
        WHERE r.activo = 1 AND r.dia_semana = ? AND (c.completado IS NULL OR c.completado = 0)
    """, (hoy_sql, hoy.weekday())).fetchall()
    for r in recurr_rows:
        d = dict(r)
        d['tipo'] = 'RECURRENTE'
        d['descripcion'] = d.get('descripcion') or 'Tarea programada'
        d['fecha_vencimiento'] = hoy_sql
        d['prioridad'] = 'ALTA'
        d['alert_id'] = f"RECURRENTE_{d['id']}_{hoy_sql}"
        d['seccion'] = 'recurrentes'
        all_alertas.append(d)

    # 5. Visitas planificadas para hoy (crm_planificacion DIARIA, pendientes)
    plan_filter = ""
    plan_params = [hoy_sql]
    if where_parts:
        plan_filter = f"""
            AND EXISTS (
                SELECT 1 FROM fact_avance_cliente_vendedor_month av
                WHERE av.cod_cliente = p.cod_cliente
                  AND av.year_month = (SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month)
                  AND {where_parts}
            )
        """
        plan_params = [hoy_sql] + params
    plan_rows = conn.execute(f"""
        SELECT p.id, p.cod_cliente, p.objetivo as descripcion, dc.cliente_name as nom_cliente
        FROM crm_planificacion p
        LEFT JOIN dim_clients dc ON p.cod_cliente = dc.cliente_id
        WHERE p.tipo = 'DIARIA' AND p.fecha = ? AND (p.completado IS NULL OR p.completado = 0)
        {plan_filter}
    """, plan_params).fetchall()
    for r in plan_rows:
        d = dict(r)
        d['tipo'] = 'TAREA_PLANIFICADA'
        d['descripcion'] = d.get('descripcion') or 'Tarea programada'
        d['fecha_vencimiento'] = hoy_sql
        d['prioridad'] = 'ALTA'
        d['alert_id'] = f"PLAN_{d['id']}_{hoy_sql}"
        d['seccion'] = 'planificacion_hoy'
        all_alertas.append(d)

    all_alertas = _filter_dismissed(conn, all_alertas)
    conn.close()

    deuda_hoy = [a for a in all_alertas if a.get('seccion') == 'deuda_hoy']
    deuda_manana = [a for a in all_alertas if a.get('seccion') == 'deuda_manana']
    gestiones = [a for a in all_alertas if a.get('seccion') == 'gestiones']
    recurrentes = [a for a in all_alertas if a.get('seccion') == 'recurrentes']
    planificacion_hoy = [a for a in all_alertas if a.get('seccion') == 'planificacion_hoy']

    return jsonify({
        'deuda_hoy': deuda_hoy,
        'deuda_manana': deuda_manana,
        'gestiones': gestiones,
        'recurrentes': recurrentes,
        'planificacion_hoy': planificacion_hoy,
        'total': len(all_alertas),
    })


@app.route('/api/alertas/dismiss', methods=['POST'])
@login_required
def api_alertas_dismiss():
    """Mark an alert as dismissed so it no longer appears."""
    data = request.get_json() or {}
    alert_id = data.get('alert_id')
    if not alert_id:
        return jsonify({'ok': False, 'error': 'alert_id requerido'}), 400
    conn = get_db()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO crm_alertas_dismissed (alert_id, user_id) VALUES (?, ?)",
            (alert_id, session.get('user', 'unknown'))
        )
        conn.commit()
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500
    finally:
        conn.close()


@app.route('/api/objetivos/mensual', methods=['GET', 'POST'])
def api_objetivos_mensual():
    """
    GET: Devuelve los 13 objetivos globales del vendedor para el mes activo.
    POST: Actualiza los objetivos globales del vendedor y recalcula el prorrateo por cliente (sólo P/Premium).
    """
    cod_vendedor = request.args.get('vendedor', '')
    if not cod_vendedor:
        return jsonify({'error': 'vendedor requerido'}), 400

    conn = get_db()
    cur_ym_row = conn.execute("SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month").fetchone()
    if not cur_ym_row:
        conn.close()
        return jsonify({'error': 'No hay mes activo'}), 404
    cur_ym = cur_ym_row[0]

    if request.method == 'GET':
        row = conn.execute("""
            SELECT 
                objetivo_pesos, objetivo_premium_pesos, objetivo_kg, objetivo_rebozados_kg,
                obj_hg, obj_sch, obj_unt, obj_rb, obj_sj, obj_grasa, obj_picada, obj_papas, obj_atun, obj_chorizos
            FROM vendedor_objetivos
            WHERE cod_vendedor = ? AND year_month = ?
        """, (cod_vendedor, cur_ym)).fetchone()
        
        conn.close()
        if not row:
            return jsonify({
                'objetivo_pesos': 0, 'objetivo_premium_pesos': 0, 'objetivo_kg': 0, 'objetivo_rebozados_kg': 0,
                'obj_hg': 0, 'obj_sch': 0, 'obj_unt': 0, 'obj_rb': 0, 'obj_sj': 0, 
                'obj_grasa': 0, 'obj_picada': 0, 'obj_papas': 0, 'obj_atun': 0, 'obj_chorizos': 0
            })
            
        return jsonify(dict(row))

    elif request.method == 'POST':
        data = request.json
        obj_pesos = float(data.get('objetivo_pesos', 0))
        obj_premium = float(data.get('objetivo_premium_pesos', 0))
        obj_kg = float(data.get('objetivo_kg', 0))
        obj_reb_kg = float(data.get('objetivo_rebozados_kg', 0))

        # 10 volume categories
        obj_hg = float(data.get('obj_hg', 0))
        obj_sch = float(data.get('obj_sch', 0))
        obj_unt = float(data.get('obj_unt', 0))
        obj_rb = float(data.get('obj_rb', 0))
        obj_sj = float(data.get('obj_sj', 0))
        obj_grasa = float(data.get('obj_grasa', 0))
        obj_picada = float(data.get('obj_picada', 0))
        obj_papas = float(data.get('obj_papas', 0))
        obj_atun = float(data.get('obj_atun', 0))
        obj_chori = float(data.get('obj_chorizos', 0))

        # 1. Update global vendor objectives
        conn.execute("""
            INSERT OR REPLACE INTO vendedor_objetivos 
            (cod_vendedor, nom_vendedor, year_month, objetivo_pesos, objetivo_premium_pesos, objetivo_kg, objetivo_rebozados_kg,
             obj_hg, obj_sch, obj_unt, obj_rb, obj_sj, obj_grasa, obj_picada, obj_papas, obj_atun, obj_chorizos)
            VALUES (
                ?, (SELECT nom_vendedor FROM fact_avance_cliente_vendedor_month WHERE cod_vendedor = ? LIMIT 1), ?, 
                ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        """, (cod_vendedor, cod_vendedor, cur_ym, 
              obj_pesos, obj_premium, obj_kg, obj_reb_kg,
              obj_hg, obj_sch, obj_unt, obj_rb, obj_sj, obj_grasa, obj_picada, obj_papas, obj_atun, obj_chori))

        # 2. Recalculate proportional distribution (pesos/premium) for clients of this vendor
        if obj_kg > 0:
            clients = conn.execute("""
                SELECT cod_cliente, objetivo 
                FROM fact_avance_cliente_vendedor_month
                WHERE cod_vendedor = ? AND year_month = ?
            """, (cod_vendedor, cur_ym)).fetchall()
            
            for cod_cli, cliente_kg in clients:
                if not cliente_kg or cliente_kg == 0:
                    continue
                # Proportional allocation based on kg
                proportion = cliente_kg / obj_kg
                cliente_obj_pesos = obj_pesos * proportion
                cliente_obj_premium = obj_premium * proportion
                
                conn.execute("""
                    UPDATE fact_avance_cliente_vendedor_month
                    SET objetivo_pesos = ?, objetivo_premium_pesos = ?
                    WHERE cod_cliente = ? AND cod_vendedor = ? AND year_month = ?
                """, (cliente_obj_pesos, cliente_obj_premium, cod_cli, cod_vendedor, cur_ym))
                
        conn.commit()
        conn.close()
        return jsonify({'status': 'success', 'message': 'Objetivos guardados correctamente'})


if __name__ == '__main__':
    import socket
    import subprocess
    ips = []
    try:
        r = subprocess.run(['hostname', '-I'], capture_output=True, text=True, timeout=2)
        if r.returncode == 0 and r.stdout.strip():
            ips = r.stdout.strip().split()
    except Exception:
        pass
    if not ips:
        try:
            ips = [ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith('127.')]
        except Exception:
            ips = []
    if not ips:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ips = [s.getsockname()[0]]
            s.close()
        except Exception:
            ips = ["?"]
    # Priorizar LAN (192.168.x, 10.x) para móvil; 100.64.x es Tailscale/VPN
    lan = [ip for ip in ips if ip.startswith(('192.168.', '10.'))]
    red_ips = lan if lan else ips
    print(f"Database: {DB_PATH}")
    print(f"Local:    http://localhost:5000")
    for ip in red_ips[:3]:
        print(f"Red:      http://{ip}:5000  (móvil en misma WiFi)")
    app.run(debug=True, host='0.0.0.0', port=5000)
