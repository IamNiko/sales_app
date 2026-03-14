from flask import Blueprint, render_template, request, jsonify
from core.db import get_db
from core.auth import login_required
from services.sales_service import SalesService

dashboard_bp = Blueprint('dashboard', __name__)

@dashboard_bp.route('/filters')
@login_required
def filters():
    return render_template('filters.html')

@dashboard_bp.route('/dashboard')
@login_required
def dashboard_page():
    return render_template('dashboard.html')

@dashboard_bp.route('/api/filters')
@login_required
def api_filters():
    conn = get_db()
    zonas = conn.execute("SELECT DISTINCT zona FROM fact_avance_cliente_vendedor_month WHERE zona IS NOT NULL ORDER BY zona").fetchall()
    jefes = conn.execute("SELECT DISTINCT zona, jefe FROM fact_avance_cliente_vendedor_month WHERE jefe IS NOT NULL ORDER BY zona, jefe").fetchall()
    vendedores = conn.execute("SELECT DISTINCT zona, jefe, cod_vendedor, nom_vendedor FROM fact_avance_cliente_vendedor_month WHERE nom_vendedor IS NOT NULL ORDER BY zona, jefe, nom_vendedor").fetchall()
    conn.close()
    return jsonify({
        'zonas': [dict(r) for r in zonas],
        'jefes': [dict(r) for r in jefes],
        'vendedores': [dict(r) for r in vendedores]
    })

# Add more dashboard routes...
