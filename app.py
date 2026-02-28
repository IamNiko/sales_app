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


# ==================== API ENDPOINTS ====================


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


@app.route('/api/dashboard')
def api_dashboard():
    """Return KPIs and chart data for a vendedor, jefe, or zona."""
    cod_vendedor = request.args.get('vendedor', '')
    jefe = request.args.get('jefe', '')
    zona = request.args.get('zona', '')
    
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
            s.tier
        FROM fact_avance_cliente_vendedor_month av
        LEFT JOIN fact_client_segmentation s ON av.cod_cliente = s.cod_cliente AND av.year_month = s.year_month
        WHERE av.{where_clause} AND av.year_month = (
            SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month
        )
        ORDER BY COALESCE(av.objetivo, 0) DESC
        LIMIT 100
    """, params).fetchall()


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
        # Get context year/month from DB
        ym_res = conn.execute("SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month").fetchone()
        ym = ym_res[0] if ym_res else datetime.now().strftime('%Y-%m')
        year, month = map(int, ym.split('-'))
        days_in_month = calendar.monthrange(year, month)[1]
        
        dates = list(range(1, days_in_month + 1))
        dates_s = [str(d) for d in dates]
        
        # Ideal Line (Linear)
        total_objetivo = summary['objetivo'] or 0
        daily_goal = total_objetivo / days_in_month if days_in_month else 0
        ideal = [round(daily_goal * d, 0) for d in dates]
        
        # Actual Line (Cumulative)
        # Map existing daily sales (sparse) to full month (dense)
        daily_map = {x['dia']: x['acumulado'] for x in daily_sales}
        actual = []
        last_val = 0
        
        # Determine cutoff day (today if current month, else end of month)
        today = datetime.now().day
        is_current_month = (ym == datetime.now().strftime('%Y-%m'))
        max_day = today if is_current_month else days_in_month
        
        # If max_day > days_in_month (Month boundary edge), clamp it
        max_day = min(max_day, days_in_month)
        
        for d in dates:
            if d <= max_day:
                val = daily_map.get(d)
                if val is not None:
                    last_val = val
                actual.append(round(last_val, 0))
            else:
                break # Future
                
        # Projection Line (Start AFTER actual points)
        projection = [None] * len(actual)
        if actual and total_objetivo > 0 and max_day > 0 and max_day < days_in_month:
            current_total = actual[-1]
            avg_daily = current_total / max_day
            remaining_days = days_in_month - max_day
            
            # Linear projection from last actual point
            for i in range(1, remaining_days + 1):
                projection.append(round(current_total + (avg_daily * i), 0))

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


    conn.close()
    
    total_facturado = summary['facturacion'] or 0
    total_pendiente = summary['pendiente'] or 0
    # total_objetivo already defined above
    pct = round((total_facturado + total_pendiente) / total_objetivo * 100, 1) if total_objetivo else 0
    
    # Determine display name
    display_name = summary['nom_vendedor'] if cod_vendedor else (jefe if jefe else zona)
    
    return jsonify({
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

    # Days logic
    import calendar
    last_day = calendar.monthrange(target_date.year, target_date.month)[1]
    days_remaining = 0
    current = target_date
    while current.day <= last_day:
        if current.weekday() < 5: # Mon-Fri
            days_remaining += 1
        current += timedelta(days=1)
    
    conn.close()
        
    daily_needed = gap / days_remaining if days_remaining > 0 else gap
    daily_avg = total_obj / 20 # Simple average target
    
    return jsonify({
        'date': date_str,
        'weekday_name': target_date.strftime('%A'),
        'target_frequencies': target_frecuencias,
        'clients': planning_clients,
        'stats': {
            'count': total_clients,
            'clients_objective_sum': total_objective, 
            'clients_historical_sum': total_potential_hist,
            'global_objective': total_obj,
            'gap': gap,
            'days_remaining': days_remaining,
            'daily_needed': daily_needed,
            'daily_average': daily_avg
        }
    })


@app.route('/api/cliente/<cod_cliente>')
def api_cliente(cod_cliente):
    """Return client detail with historical data and product breakdown."""
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
    
    # Get historical sales from fact_cliente_historico (from Jan 2025)
    historia_rows = conn.execute("""
        SELECT 
            year_month,
            kg_vendidos as total_kg
        FROM fact_cliente_historico
        WHERE cod_cliente = ? AND year_month >= '2025-01'
        ORDER BY year_month ASC
    """, (cod_cliente,)).fetchall()
    
    historia = [dict(h) for h in historia_rows]
    
    # Add current month data from fact_facturacion if not present in history
    if current_ym and not any(h['year_month'] == current_ym for h in historia):
        current_kg = conn.execute("""
            SELECT SUM(cantidad) 
            FROM fact_facturacion 
            WHERE cod_cliente = ? AND year_month = ?
        """, (cod_cliente, current_ym)).fetchone()[0] or 0
        
        historia.append({
            'year_month': current_ym,
            'total_kg': current_kg
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
    
    # Calculate weighted objetivo for rebozados if we have vendor info
    objetivo_rebozados_kg = 0
    if avance and vendedor_arg and vendedor_arg != 'undefined':
        # Get vendor's total rebozados objective
        vendor_obj = conn.execute("""
            SELECT objetivo_rebozados_kg, objetivo_kg 
            FROM vendedor_objetivos 
            WHERE cod_vendedor = ? 
            ORDER BY year_month DESC LIMIT 1
        """, (vendedor_arg,)).fetchone()
        
        if vendor_obj and vendor_obj['objetivo_kg'] > 0:
            # Weight client rebozados objetivo by their % of total KG
            client_kg_objetivo = avance['objetivo'] or 0
            vendor_kg_objetivo = vendor_obj['objetivo_kg']
            vendor_rebozados_objetivo = vendor_obj['objetivo_rebozados_kg'] or 0
            
            # Proportional allocation
            if vendor_kg_objetivo > 0:
                objetivo_rebozados_kg = (client_kg_objetivo / vendor_kg_objetivo) * vendor_rebozados_objetivo
    
    # Merge into avance dict
    avance_dict = dict(avance) if avance else {}
    if current_sales:
        avance_dict['facturacion_pesos'] = current_sales['venta_actual_pesos'] or 0
        avance_dict['premium_pesos'] = current_sales['venta_premium_pesos'] or 0
        avance_dict['rebozados_kg'] = current_sales['rebozados_kg'] or 0
    
    avance_dict['objetivo_rebozados_kg'] = round(objetivo_rebozados_kg, 2)
    
    conn.close()
    
    return jsonify({
        'cliente': dict(cliente) if cliente else None,
        'avance': avance_dict if avance_dict else None,
        'historia': [dict(h) for h in historia],
        'categorias': [dict(c) for c in categorias],
        'productos': [dict(p) for p in productos]
    })


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
        SELECT lanzamiento, cod_cliente, nom_cliente, estado, fact_feb, pend_feb, total_feb, promedio_u3
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


if __name__ == '__main__':
    print(f"Database: {DB_PATH}")
    print(f"Starting server at http://localhost:5000")
    app.run(debug=True, host='0.0.0.0', port=5000)
