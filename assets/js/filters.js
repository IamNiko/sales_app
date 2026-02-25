/**
 * filters.js - Dashboard filtering logic
 */

const FilterManager = {
    current: {
        region: 'all',
        product: 'all',
        seller: 'all'
    },

    init() {
        document.getElementById('filter-region').addEventListener('change', (e) => this.handleFilter('region', e.target.value));
        document.getElementById('filter-product').addEventListener('change', (e) => this.handleFilter('product', e.target.value));
        document.getElementById('filter-seller').addEventListener('change', (e) => this.handleFilter('seller', e.target.value));

        document.getElementById('reset-filters').addEventListener('click', () => {
            this.reset();
            DashboardApp.updateUI();
        });
    },

    handleFilter(type, value) {
        this.current[type] = value;
        DashboardApp.updateUI();
    },

    reset() {
        this.current = { region: 'all', product: 'all', seller: 'all' };
        document.getElementById('filter-region').value = 'all';
        document.getElementById('filter-product').value = 'all';
        document.getElementById('filter-seller').value = 'all';
    },

    populateFilters() {
        this.populateSelect('filter-region', DataModel.getUniqueValues('region'), 'All Regions');
        this.populateSelect('filter-product', DataModel.getUniqueValues('product'), 'All Products');
        this.populateSelect('filter-seller', DataModel.getUniqueValues('seller'), 'All Sellers');
    },

    populateSelect(id, values, defaultText) {
        const select = document.getElementById(id);
        select.innerHTML = `<option value="all">${defaultText}</option>`;
        values.forEach(val => {
            const opt = document.createElement('option');
            opt.value = val;
            opt.textContent = val;
            select.appendChild(opt);
        });
    }
};
