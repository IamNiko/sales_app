/**
 * model.js - Semantic data model and KPI calculations
 */

const DataModel = {
    data: [], // Sales[]
    filtered: [],

    setRows(rows) {
        this.data = rows.map(row => ({
            ...row,
            date: this.parseExcelDate(row.date),
            amount: parseFloat(row.amount || 0),
            units: parseInt(row.units || 0),
            coverage: parseFloat(row.coverage || 0),
            target: parseFloat(row.target || 0)
        })).filter(r => !isNaN(r.amount) && r.amount !== 0);

        this.data.sort((a, b) => a.date - b.date); // Ensure chronological order
        this.filtered = [...this.data];
    },

    parseExcelDate(excelDate) {
        if (!excelDate) return new Date();
        // Handle numeric date format from Excel
        if (!isNaN(excelDate)) {
            return new Date((excelDate - 25569) * 86400 * 1000);
        }
        const d = new Date(excelDate);
        return isNaN(d.getTime()) ? new Date() : d;
    },

    getKPIs(dataset = this.filtered) {
        const totalSales = dataset.reduce((sum, r) => sum + r.amount, 0);
        const totalUnits = dataset.reduce((sum, r) => sum + r.units, 0);
        const avgTicket = totalUnits > 0 ? totalSales / totalUnits : 0;

        // Coverage is often row-based, so average of rows is acceptable for now
        const avgCoverage = dataset.length > 0
            ? (dataset.reduce((sum, r) => sum + r.coverage, 0) / dataset.length) * 100
            : 0;

        // Previous Period Calculation (Simple: comparison with same duration before start date)
        let trend = 0;
        if (dataset.length > 0) {
            const startDate = dataset[0].date;
            const endDate = dataset[dataset.length - 1].date;
            const duration = endDate - startDate;
            const prevStart = new Date(startDate.getTime() - duration);

            // Find previous period rows in FULL dataset (not just filtered, unless regional filter applies)
            // Ideally we should apply current filters to the full dataset but with different date range
            // For MVP we just return 0 if complicated, or implementing a basic logic:

            // We need to know the *current* filter context to do this right.
            // For now, let's leave trend as 0 or implement a simple "first half vs second half" if only one period is selected?
            // Actually, best is: if filtered data has > 1 month, compare last month vs previous.
            // Let's keep it simple: Compare vs previous month of data present in the selection?
            // No, standard is: (Current Sum / Previous Sum) - 1. 
            // Let's implement a 'comparePrevious' method later.
        }

        return {
            totalSales,
            totalUnits,
            avgTicket,
            avgCoverage,
            trend // 0 for now
        };
    },

    getUniqueValues(field) {
        const values = new Set();
        this.data.forEach(r => {
            if (r[field]) values.add(r[field]);
        });
        return Array.from(values).sort();
    },

    filter(filters) {
        this.filtered = this.data.filter(row => {
            let match = true;
            if (filters.region && filters.region !== 'all') match = match && row.region === filters.region;
            if (filters.product && filters.product !== 'all') match = match && row.product === filters.product;
            if (filters.seller && filters.seller !== 'all') match = match && row.seller === filters.seller;
            // Date filter logic would go here
            return match;
        });
        return this.filtered;
    }
};
