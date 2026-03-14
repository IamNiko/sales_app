from flask import Blueprint, render_template, request, redirect, url_for, session
from core.db import get_db

auth_bp = Blueprint('auth', __name__)

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    """Simple login page."""
    if request.method == 'POST':
        # Simple auth - in production use proper auth
        username = request.form.get('username', '')
        if username:
            session['user'] = username
            return redirect(url_for('dashboard.filters')) # Target after login
    return render_template('login.html')

@auth_bp.route('/logout')
def logout():
    """Logout and clear session."""
    session.clear()
    return redirect(url_for('auth.login'))

@auth_bp.route('/')
def index():
    """Redirect to login or filters."""
    if 'user' in session:
        return redirect(url_for('dashboard.filters'))
    return redirect(url_for('auth.login'))
