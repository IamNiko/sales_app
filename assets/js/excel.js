/**
 * excel.js - Handles file loading and SheetJS parsing
 */

const ExcelEngine = {
    mapping: null,

    async init() {
        try {
            const response = await fetch('data/mapping.json');
            this.mapping = await response.json();
        } catch (error) {
            console.error('Failed to load mapping.json:', error);
            alert('Critial: mapping.json missing. Data normalization will fail.');
        }
    },

    readExcel(file) {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = (e) => {
                const data = new Uint8Array(e.target.result);
                const workbook = XLSX.read(data, { type: 'array' });

                // We take the first sheet by default
                const firstSheetName = workbook.SheetNames[0];
                const worksheet = workbook.Sheets[firstSheetName];

                // Convert to JSON
                const jsonData = XLSX.utils.sheet_to_json(worksheet, { header: 1 });

                if (jsonData.length < 2) {
                    reject('Excel file seems empty or has no header row.');
                    return;
                }

                resolve({
                    headers: jsonData[0],
                    rows: jsonData.slice(1),
                    fileName: file.name
                });
            };
            reader.onerror = reject;
            reader.readAsArrayBuffer(file);
        });
    },

    normalizeData(headers, rawRows) {
        if (!this.mapping) return rawRows;

        const headerMap = {};
        headers.forEach((h, index) => {
            const cleanHeader = String(h).toLowerCase().trim();

            // Find canonical field for this header
            for (const [canonical, aliases] of Object.entries(this.mapping.fields)) {
                if (aliases.includes(cleanHeader) || canonical === cleanHeader) {
                    headerMap[canonical] = index;
                    break;
                }
            }
        });

        // Check required fields
        // Check required fields
        const missing = this.mapping.required.filter(req => headerMap[req] === undefined);

        if (missing.length > 0) {
            console.error('Missing columns:', missing);
            console.log('Available headers:', headers);

            alert(
                `Error: The file is missing required columns: ${missing.join(', ')}\n\n` +
                `Please ensure your Excel file has headers like: ${this.mapping.fields[missing[0]].join(', ')}`
            );
            throw new Error(`Missing columns: ${missing.join(', ')}`);
        }

        if (rawRows.length === 0) {
            alert("Error: The file contains no data rows.");
            throw new Error("Empty file");
        }

        return rawRows.map(row => {
            const normalizedRow = {};
            for (const canonical in this.mapping.fields) {
                const colIndex = headerMap[canonical];
                normalizedRow[canonical] = colIndex !== undefined ? row[colIndex] : null;
            }
            return normalizedRow;
        });
    }
};

// Auto-init engine
ExcelEngine.init();
