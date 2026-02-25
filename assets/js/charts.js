/**
 * charts.js - ECharts configuration and rendering
 */

const ChartEngine = {
    instances: {},
    colors: ['#3498db', '#2ecc71', '#e74c3c', '#f1c40f', '#9b59b6', '#34495e'],

    init() {
        // Initialize chart instances
        this.instances.salesTime = echarts.init(document.getElementById('chart-sales-time'));
        this.instances.salesRegion = echarts.init(document.getElementById('chart-sales-region'));
        this.instances.salesProduct = echarts.init(document.getElementById('chart-sales-product'));
        this.instances.salesCategory = echarts.init(document.getElementById('chart-sales-category'));

        window.addEventListener('resize', () => {
            Object.values(this.instances).forEach(chart => chart.resize());
        });
    },

    updateAll(data) {
        this.renderSalesTime(data);
        this.renderSalesRegion(data);
        this.renderSalesProduct(data);
        this.renderSalesCategory(data);
    },

    commonOptions() {
        return {
            textStyle: { fontFamily: 'Inter, sans-serif' },
            grid: { top: 40, right: 20, bottom: 40, left: 60, containLabel: true },
            tooltip: {
                trigger: 'axis',
                backgroundColor: 'rgba(255, 255, 255, 0.95)',
                borderColor: '#eee',
                textStyle: { color: '#333' },
                shadowBlur: 10,
                shadowColor: 'rgba(0,0,0,0.1)'
            }
        };
    },

    renderSalesTime(data) {
        // Group by date
        const timeline = {};
        data.forEach(r => {
            const day = r.date.toISOString().split('T')[0];
            timeline[day] = (timeline[day] || 0) + r.amount;
        });

        const sortedDays = Object.keys(timeline).sort();
        const values = sortedDays.map(d => timeline[d]);

        this.instances.salesTime.setOption({
            ...this.commonOptions(),
            tooltip: { ...this.commonOptions().tooltip, trigger: 'axis' },
            xAxis: {
                type: 'category',
                data: sortedDays,
                axisLine: { show: false },
                axisTick: { show: false },
                axisLabel: { color: '#95a5a6' }
            },
            yAxis: {
                type: 'value',
                splitLine: { lineStyle: { type: 'dashed', color: '#eee' } },
                axisLabel: {
                    formatter: (value) => value >= 1000 ? (value / 1000).toFixed(0) + 'k' : value,
                    color: '#95a5a6'
                }
            },
            series: [{
                data: values,
                type: 'line',
                smooth: true,
                symbol: 'none',
                lineStyle: { width: 3, color: '#3498db' },
                areaStyle: {
                    color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                        { offset: 0, color: 'rgba(52, 152, 219, 0.2)' },
                        { offset: 1, color: 'rgba(52, 152, 219, 0.0)' }
                    ])
                },
                itemStyle: { color: '#3498db' }
            }]
        });
    },

    renderSalesRegion(data) {
        const regions = {};
        data.forEach(r => {
            if (r.region) regions[r.region] = (regions[r.region] || 0) + r.amount;
        });

        const sorted = Object.entries(regions).sort((a, b) => b[1] - a[1]);

        this.instances.salesRegion.setOption({
            ...this.commonOptions(),
            tooltip: { ...this.commonOptions().tooltip, trigger: 'item' },
            grid: { top: 10, right: 30, bottom: 20, left: 10, containLabel: true },
            xAxis: {
                type: 'value',
                splitLine: { show: false },
                axisLabel: { color: '#95a5a6' }
            },
            yAxis: {
                type: 'category',
                data: sorted.map(i => i[0]).reverse(),
                axisLine: { show: false },
                axisTick: { show: false },
                axisLabel: { color: '#2c3e50', fontWeight: 600 }
            },
            series: [{
                data: sorted.map(i => i[1]).reverse(),
                type: 'bar',
                barWidth: '60%',
                itemStyle: {
                    color: '#2c3e50',
                    borderRadius: [0, 4, 4, 0]
                }
            }]
        });
    },

    renderSalesProduct(data) {
        const products = {};
        data.forEach(r => {
            if (r.product) products[r.product] = (products[r.product] || 0) + r.amount;
        });

        const sorted = Object.entries(products).sort((a, b) => b[1] - a[1]).slice(0, 10);

        this.instances.salesProduct.setOption({
            ...this.commonOptions(),
            tooltip: { ...this.commonOptions().tooltip, trigger: 'item' },
            grid: { top: 10, right: 30, bottom: 20, left: 10, containLabel: true },
            xAxis: {
                type: 'value',
                splitLine: { show: false },
                axisLabel: { show: false }
            },
            yAxis: {
                type: 'category',
                data: sorted.map(i => i[0]).reverse(),
                axisLine: { show: false },
                axisTick: { show: false },
                axisLabel: { color: '#2c3e50', width: 90, overflow: 'truncate' }
            },
            series: [{
                data: sorted.map(i => i[1]).reverse(),
                type: 'bar',
                barWidth: '60%',
                itemStyle: {
                    color: '#3498db',
                    borderRadius: [0, 4, 4, 0]
                },
                label: {
                    show: true,
                    position: 'right',
                    formatter: (params) => (params.value / 1000).toFixed(1) + 'k'
                }
            }]
        });
    },

    renderSalesCategory(data) {
        const categories = {};
        data.forEach(r => {
            if (r.category) categories[r.category] = (categories[r.category] || 0) + r.amount;
        });

        this.instances.salesCategory.setOption({
            ...this.commonOptions(),
            tooltip: { trigger: 'item' },
            color: this.colors,
            series: [{
                type: 'pie',
                radius: ['40%', '70%'],
                center: ['50%', '50%'],
                itemStyle: {
                    borderRadius: 5,
                    borderColor: '#fff',
                    borderWidth: 2
                },
                label: { show: false },
                data: Object.entries(categories).map(([name, value]) => ({ name, value })),
                emphasis: {
                    label: { show: true, fontSize: '14', fontWeight: 'bold' },
                    itemStyle: { shadowBlur: 10, shadowOffsetX: 0, shadowColor: 'rgba(0, 0, 0, 0.2)' }
                }
            }]
        });
    }
};
