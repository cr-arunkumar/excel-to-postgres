import { faker } from '@faker-js/faker';
import ExcelJS from 'exceljs';
import { Worker, isMainThread, parentPort, workerData } from 'worker_threads';
import os from 'os';

// Constants
const NUM_COLUMNS = 100;
const NUM_ROWS = 200_000;
const FILE_NAME = 'fakeData.xlsx';
const NUM_WORKERS = Math.max(1, os.cpus().length - 1);
const ROWS_PER_WORKER = Math.ceil(NUM_ROWS / NUM_WORKERS);

if (!isMainThread) {
    const { startRow, endRow, columns } = workerData;
    const rows = [];
    for (let i = startRow; i < endRow; i++) {
        const rowData: { [key: string]: any } = {};
        columns.forEach((col: { key: string | number; }, index: number) => {
            rowData[col.key] = index === 0 ? faker.lorem.sentence() : faker.finance.accountName();
        });
        rows.push(rowData);
    }
    parentPort?.postMessage(rows);
} else {
    async function generateExcel() {
        const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({ filename: FILE_NAME });
        const worksheet = workbook.addWorksheet('Fake Data');

        const columns = Array.from({ length: NUM_COLUMNS }, (_, i) => ({
            header: `Column ${i + 1}`,
            key: `column${i + 1}`,
            width: 15
        }));
        worksheet.columns = columns;

        const workerPromises = Array.from({ length: NUM_WORKERS }, (_, i) => {
            const startRow = i * ROWS_PER_WORKER;
            const endRow = Math.min(startRow + ROWS_PER_WORKER, NUM_ROWS);
            return new Promise<any[]>((resolve, reject) => {
                const worker = new Worker(__filename, {
                    workerData: { startRow, endRow, columns }
                });
                worker.on('message', resolve);
                worker.on('error', reject);
            });
        });

        const results = await Promise.all(workerPromises);
        results.flat().forEach(rowData => worksheet.addRow(rowData).commit());

        await workbook.commit();
        console.log(`Excel file generated: ${FILE_NAME}`);
    }

    generateExcel().catch(console.error);
}