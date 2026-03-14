from datetime import datetime, timedelta
import calendar
from core.db import get_db

class CRMService:
    @staticmethod
    def get_planning_data(vendedor=None, jefe=None, zona=None, date_str=None):
        if not date_str:
            date_str = datetime.now().strftime('%Y-%m-%d')
            
        target_date = datetime.strptime(date_str, '%Y-%m-%d')
        weekday = target_date.weekday()
        
        mapping = {
            0: ['MARTES', 'MIERCOLES'],
            1: ['MIERCOLES', 'JUEVES'],
            2: ['JUEVES', 'VIERNES'],
            3: ['VIERNES', 'LUNES'],
            4: ['LUNES', 'MARTES'],
            5: [], 6: []
        }
        target_frecuencias = mapping.get(weekday, [])
        
        conn = get_db()
        params = []
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
        
        planning_clients = []
        for c in all_clients:
            freq = (c['frecuencia'] or '').upper()
            if freq in target_frecuencias:
                planning_clients.append(dict(c))
                
        # Total potential based on history
        client_codes = [c['cod_cliente'] for c in planning_clients]
        hist_map = {}
        if client_codes:
            placeholders = ','.join(['?'] * len(client_codes))
            hist_rows = conn.execute(f"SELECT cod_cliente, AVG(kg_vendidos) as avg_kg FROM fact_cliente_historico WHERE cod_cliente IN ({placeholders}) GROUP BY cod_cliente", client_codes).fetchall()
            hist_map = {r['cod_cliente']: (r['avg_kg'] or 0) for r in hist_rows}
            
        for c in planning_clients:
            c['historico'] = hist_map.get(c['cod_cliente'], 0)

        conn.close()
        return {
            'date': date_str,
            'clients': planning_clients,
            'target_frequencies': target_frecuencias
        }

    @staticmethod
    def get_client_detail(cod_cliente, meses=6, vendedor_arg=None):
        conn = get_db()
        # Full logic from api_cliente goes here later
        conn.close()
        return {}
