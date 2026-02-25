/**
 * app.js - Main dashboard orchestrator
 */

const DashboardApp = {
    async init() {
        console.log('Dashboard Initialing...');

        // Initialize sub-modules
        ChartEngine.init();
        FilterManager.init();

        // Handle File Upload
        const fileInput = document.getElementById('excel-file');
        fileInput.addEventListener('change', async (e) => {
            const file = e.target.files[0];
            if (!file) return;

            try {
                const data = await ExcelEngine.readExcel(file);
                const normalized = ExcelEngine.normalizeData(data.headers, data.rows);

                DataModel.setRows(normalized);
                FilterManager.populateFilters();
                this.updateUI();

                console.log(`Loaded ${data.rows.length} rows from ${data.fileName}`);
            } catch (err) {
                console.error('Core Error:', err);
                alert('Error loading Excel: ' + err);
            }
        });

        // Export handle
        document.getElementById('export-csv').addEventListener('click', () => this.exportToCSV());
    },

    updateUI() {
        const filteredData = DataModel.filter(FilterManager.current);
        const kpis = DataModel.getKPIs(filteredData);

        // Update KPI Cards
        document.getElementById('kpi-total-sales').textContent = this.formatCurrency(kpis.totalSales);
        document.getElementById('kpi-total-units').textContent = kpis.totalUnits.toLocaleString();
        document.getElementById('kpi-avg-ticket').textContent = this.formatCurrency(kpis.avgTicket);
        document.getElementById('kpi-coverage').textContent = kpis.avgCoverage.toFixed(1) + '%';
        document.getElementById('coverage-fill').style.width = kpis.avgCoverage + '%';

        // Update Charts
        ChartEngine.updateAll(filteredData);

        // Update Table
        this.renderTable(filteredData.slice(0, 50)); // Limit to first 50 for performance
    },

    renderTable(data) {
        const tbody = document.getElementById('table-body');
        const LIMIT = 100;
        const visibleData = data.slice(0, LIMIT);

        tbody.innerHTML = visibleData.map(r => `
            <tr>
                <td>${r.date.toLocaleDateString('es-AR')}</td>
                <td>${r.customer || 'N/A'}</td>
                <td>${r.product || 'N/A'}</td>
                <td>${r.region || 'N/A'}</td>
                <td>${r.units}</td>
                <td style="font-weight: 600">${this.formatCurrency(r.amount)}</td>
            </tr>
        `).join('');

        // Update count indicator if exists, or create it
        let countDisplay = document.getElementById('row-count-display');
        if (!countDisplay) {
            countDisplay = document.createElement('div');
            countDisplay.id = 'row-count-display';
            countDisplay.style.padding = '10px';
            countDisplay.style.color = 'var(--text-muted)';
            countDisplay.style.fontSize = '0.9rem';
            document.querySelector('.table-responsive').after(countDisplay);
        }

        const remaining = data.length - visibleData.length;
        countDisplay.innerHTML = `Showing <strong>${visibleData.length}</strong> of <strong>${data.length}</strong> rows` +
            (remaining > 0 ? ` (+${remaining} hidden)` : '');
    },

    formatCurrency(val) {
        return new Intl.NumberFormat('es-AR', { style: 'currency', currency: 'USD' }).format(val);
    },

    exportToCSV() {
        const data = DataModel.filtered;
        if (!data.length) return;

        const headers = Object.keys(data[0]);
        const csvContent = [
            headers.join(','),
            ...data.map(row => headers.map(h => {
                let cell = row[h];
                if (cell instanceof Date) cell = cell.toISOString();
                return `"${String(cell).replace(/"/g, '""')}"`;
            }).join(','))
        ].join('\n');

        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement("a");
        const url = URL.createObjectURL(blob);
        link.setAttribute("href", url);
        link.setAttribute("download", `export_${new Date().getTime()}.csv`);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }
};

// Start the app when DOM is ready
document.addEventListener('DOMContentLoaded', () => DashboardApp.init());
