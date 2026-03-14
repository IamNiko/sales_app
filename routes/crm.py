from flask import Blueprint, render_template, request, jsonify
from core.db import get_db
from core.auth import login_required
from services.crm_service import CRMService

crm_bp = Blueprint('crm', __name__)

@crm_bp.route('/crm')
@login_required
def crm_page():
    return render_template('crm.html')

@crm_bp.route('/planning')
@login_required
def planning_page():
    return render_template('planning.html')

@crm_bp.route('/api/planning')
@login_required
def api_planning():
    vendedor = request.args.get('vendedor')
    jefe = request.args.get('jefe')
    zona = request.args.get('zona')
    date_str = request.args.get('date')
    
    data = CRMService.get_planning_data(vendedor, jefe, zona, date_str)
    return jsonify(data)

@crm_bp.route('/api/cliente/<cod_cliente>', methods=['GET', 'PUT'])
@login_required
def api_cliente(cod_cliente):
    if request.method == 'PUT':
        # Update logic...
        return jsonify({'status': 'success'})
    
    meses = int(request.args.get('meses', 6))
    vendedor = request.args.get('vendedor')
    data = CRMService.get_client_detail(cod_cliente, meses, vendedor)
    return jsonify(data)
