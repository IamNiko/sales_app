# Sales Intelligence Dashboard

A professional, offline, local-only sales dashboard that runs entirely in the browser.

## Features
- **Local-first**: No cloud, no servers, no internet required.
- **Excel Support**: Load `.xlsx` files directly from your computer.
- **Dynamic Mapping**: Automatic normalization of Excel headers via `data/mapping.json`.
- **Interactive Analytics**: KPI cards and charts (Time Series, Regional, Product) that respond to global filters.
- **Data Export**: Export filtered data back to CSV.

## Structure
- `index.html`: Dashboard layout and grid system.
- `assets/css/app.css`: Corporate industrial theme.
- `assets/js`: Core logic modules (Excel, Model, Charts, Filters).
- `vendor`: Locally stored SheetJS and ECharts libraries.
- `data/mapping.json`: Configuration for Excel header aliases.

## How to use
1. Double-click `index.html` to open in any modern browser.
2. Click **"Load Excel"** and select your sales file.
3. Use the filters to drill down into specific regions, products, or sellers.
4. Export results if needed using the **"Export CSV"** button.

## Customizing Mapping
If your Excel columns change (e.g., instead of "Region" it says "Zone"), update the `data/mapping.json` file:
```json
"region": ["region", "zona", "territorio", "zone"]
```

## Security Note
This application processes all data in your browser's memory. No data is ever uploaded or transmitted outside your local machine.
