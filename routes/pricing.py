from flask import Blueprint, render_template, request, jsonify
from core.db import get_db
from core.auth import login_required

pricing_bp = Blueprint('pricing', __name__)

@pricing_bp.route('/pricing')
@login_required
def pricing_page():
    return render_template('pricing.html')

# API endpoints for pricing logic will be added here
# e.g., @pricing_bp.route('/api/prices')
