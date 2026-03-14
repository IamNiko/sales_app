from flask import Blueprint, jsonify, request
from services.sales_service import SalesService

api_bp = Blueprint('api', __name__, url_prefix='/api')

@api_bp.route('/welcome')
def welcome():
    vendedor = request.args.get('vendedor', '')
    jefe = request.args.get('jefe', '')
    zona = request.args.get('zona', '')
    return jsonify(SalesService.get_welcome_insights(vendedor, jefe, zona))

@api_bp.route('/meta')
def meta():
    return jsonify(SalesService.get_meta())
