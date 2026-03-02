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

    # Days logic
    import calendar
    last_day = calendar.monthrange(target_date.year, target_date.month)[1]
    month_end = target_date.replace(day=last_day)
    days_remaining = 0
    current = target_date
    while current <= month_end:
        if current.weekday() < 5:  # Mon-Fri
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
    remaining_days = max(0, days_in_month - elapsed_days)

    # ── Current-month in-progress projection (linear) ────────────
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
            avg_daily = sum(daily_vals) / elapsed_days
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

    # ── Current-month closing projection ─────────────────────────
    # For a closed month: final result = facturado + pendiente (avance pending)
    if not is_cur_month and pend_kg > 0:
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

    # ── 3. OPPORTUNITIES — active clients ranked by priority score ──
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
        ORDER BY
            (CASE s.tier WHEN 'AAA' THEN 4 WHEN 'AA' THEN 3 WHEN 'CN' THEN 3 WHEN 'A' THEN 2 ELSE 1 END) DESC,
            av.pendiente DESC
        LIMIT 8
    """, params + [cur_ym]).fetchall()

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
        for cat, lanz_name in CATEGORY_MAP.items():
            if lanz_name not in active_lanz:
                continue
            buyers = conn.execute("""
                SELECT DISTINCT f.cod_cliente
                FROM fact_facturacion f
                JOIN dim_product_classification p ON f.cod_producto = p.cod_producto
                WHERE f.year_month = ? AND p.categoria = ? AND f.cantidad > 0
            """, (fact_ym, cat)).fetchall()
            for row in buyers:
                conn.execute("""
                    UPDATE fact_lanzamiento_cobertura
                    SET estado = 'COMPRADOR'
                    WHERE cod_cliente = ? AND lanzamiento = ? AND year_month = ?
                      AND estado != 'COMPRADOR'
                """, (row['cod_cliente'], lanz_name, lanz_ym))
        for ln in RB_LANZAMIENTOS:
            if ln not in active_lanz:
                continue
            buyers = conn.execute("""
                SELECT DISTINCT f.cod_cliente
                FROM fact_facturacion f
                JOIN dim_product_classification p ON f.cod_producto = p.cod_producto
                WHERE f.year_month = ? AND p.categoria = 'REBOZADOS' AND f.cantidad > 0
            """, (fact_ym,)).fetchall()
            for row in buyers:
                conn.execute("""
                    UPDATE fact_lanzamiento_cobertura
                    SET estado = 'COMPRADOR'
                    WHERE cod_cliente = ? AND lanzamiento = ? AND year_month = ?
                      AND estado != 'COMPRADOR'
                """, (row['cod_cliente'], ln, lanz_ym))
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
    Compares coverage (unique buyers) of each launch product across the last 3
    billing months using fact_facturacion + dim_product_classification.
    This shows whether launch adoption is growing, stable or declining.
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
    cur_ym = conn.execute(
        "SELECT MAX(year_month) FROM fact_avance_cliente_vendedor_month"
    ).fetchone()[0]
    vendor_codes_rows = conn.execute(f"""
        SELECT DISTINCT cod_vendedor FROM fact_avance_cliente_vendedor_month
        WHERE {where_av} AND year_month = ?
    """, av_params + [cur_ym]).fetchall()
    vendor_codes = [r['cod_vendedor'] for r in vendor_codes_rows]

    if not vendor_codes:
        conn.close()
        return jsonify({'meses': [], 'lanzamientos': [], 'series': {}})

    # Total clients in scope (denominator for coverage %)
    total_clients = conn.execute(f"""
        SELECT COUNT(DISTINCT cod_cliente) FROM fact_avance_cliente_vendedor_month
        WHERE {where_av} AND year_month = ?
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

    # Buyers per launch product per month
    # A "launch" is identified by categoria in dim_product_classification
    # We reuse the same launch taxonomy from fact_lanzamiento_cobertura
    launch_names_rows = conn.execute(
        "SELECT DISTINCT lanzamiento FROM fact_lanzamiento_cobertura ORDER BY lanzamiento"
    ).fetchall()
    launch_names = [r['lanzamiento'] for r in launch_names_rows]

    # Map launch name → product categories that belong to it
    # (fact_lanzamiento_cobertura.lanzamiento is the product name directly)
    # We join via dim_product_classification by matching categoria/descripcion
    # Simpler: count unique buyers who purchased any product in the same category as the launch
    # For now: use dim_product_classification.categoria as proxy for launch grouping
    # and join against fact_lanzamiento_cobertura to get the correct launch names

    # Get clients per launch per month from fact_facturacion
    # Match launch products by joining fact_lanzamiento_cobertura → dim_product_classification
    rows = conn.execute(f"""
        SELECT
            f.year_month,
            lz.lanzamiento,
            COUNT(DISTINCT f.cod_cliente) as compradores,
            SUM(f.cantidad) as kg_total
        FROM fact_facturacion f
        JOIN dim_product_classification p ON f.cod_producto = p.cod_producto
        JOIN (
            SELECT DISTINCT lanzamiento,
                   SUBSTR(lanzamiento, 1, 8) as prefix
            FROM fact_lanzamiento_cobertura
        ) lz ON UPPER(p.descripcion) LIKE '%' || UPPER(SUBSTR(lz.lanzamiento, 1, 8)) || '%'
                OR UPPER(p.categoria) LIKE '%' || UPPER(SUBSTR(lz.lanzamiento, 1, 8)) || '%'
        WHERE f.cod_vendedor IN ({ph})
          AND f.year_month IN ({','.join(['?']*len(months))})
        GROUP BY f.year_month, lz.lanzamiento
        ORDER BY lz.lanzamiento, f.year_month
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

    # Fill missing months with zeros
    for lz in series:
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

    return jsonify({
        'meses': months,
        'mes_labels': mes_labels,
        'total_clientes': total_clients,
        'lanzamientos': [lz for lz in series.keys()],
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


if __name__ == '__main__':
    print(f"Database: {DB_PATH}")
    print(f"Starting server at http://localhost:5000")
    app.run(debug=True, host='0.0.0.0', port=5000)
