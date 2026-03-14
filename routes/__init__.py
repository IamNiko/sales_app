from .auth import auth_bp
from .dashboard import dashboard_bp
from .crm import crm_bp
from .pricing import pricing_bp
from .api import api_bp

def register_blueprints(app):
    app.register_blueprint(auth_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(crm_bp)
    app.register_blueprint(pricing_bp)
    app.register_blueprint(api_bp)
