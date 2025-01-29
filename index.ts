import * as XLSX from 'xlsx';
import postgres from 'postgres';
import path from 'path';
import dotenv from 'dotenv';
import * as fs from 'fs';
import { Worker, isMainThread, parentPort, workerData } from 'worker_threads';
import os from 'os';

dotenv.config();

// Constants
const CHUNK_SIZE = 25000;
const NUM_WORKERS = Math.max(1, os.cpus().length - 1);

const createDbConnection = () => postgres({
    host: process.env.DB_HOST || 'localhost',
    port: Number(process.env.DB_PORT) || 5432,
    database: process.env.DB_NAME || 'excel-postgres',
    username: process.env.DB_USER || 'prisma',
    password: process.env.DB_PASSWORD || 'topsecret',
    max: 20,
    idle_timeout: 0,
    connect_timeout: 30,
    prepare: false,

});

async function createTable(sql: postgres.Sql, tableName: string, columns: string[]): Promise<void> {
    try {
        await sql.unsafe(`DROP TABLE IF EXISTS "${tableName}" CASCADE`);
        const columnDefs = columns.map(col => `"${col}" TEXT`).join(',\n    ');
        
        const createTableSQL = `
            CREATE TABLE IF NOT EXISTS "${tableName}" (
                id SERIAL PRIMARY KEY,
                ${columnDefs},
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `;

        await sql.unsafe(createTableSQL);
        console.log('Table created successfully');
    } catch (error) {
        console.error('Error creating table:', error);
        throw error;
    }
}

async function processExcelInBatches(filePath: string, callback: (rows: any[]) => Promise<void>) {
    try {
        const workbook = XLSX.readFile(filePath, {
            cellDates: true,
            raw: true
        });
        
        const worksheet = workbook.Sheets[workbook.SheetNames[0]];
        const range = XLSX.utils.decode_range(worksheet['!ref'] || 'A1');
        const totalRows = range.e.r;
        
        console.log(`Total rows to process: ${totalRows}`);
        
        // Get headers
        const headers = XLSX.utils.sheet_to_json(worksheet, { 
            header: 1,
            range: 'A1:' + XLSX.utils.encode_cell({ r: 0, c: range.e.c })
        })[0] as string[];
        
        console.log(`Headers detected: ${headers.join(', ')}`);
        
        // Process in batches
        for (let startRow = 1; startRow <= totalRows; startRow += CHUNK_SIZE) {
            const endRow = Math.min(startRow + CHUNK_SIZE - 1, totalRows);
            const range = `A${startRow}:${XLSX.utils.encode_col(headers.length - 1)}${endRow}`;
            
            const rows = XLSX.utils.sheet_to_json(worksheet, {
                range,
                header: headers,
                raw: false,
                dateNF: 'yyyy-mm-dd'
            });
            
            if (rows.length > 0) {
                await callback(rows);
                console.log(`Processed rows ${startRow} to ${endRow}`);
            }
        }
        
        return headers;
    } catch (error) {
        console.error('Error processing Excel:', error);
        throw error;
    }
}

async function copyBatchToPostgres(sql: postgres.Sql, tableName: string, columns: string[], batch: any[]): Promise<void> {
    if (batch.length === 0) return;

    try {
        const columnList = columns.map(col => `"${col}"`).join(', ');
        const values = batch.map(row => 
            `(${columns.map(col => {
                const val = row[col];
                if (val === null || val === undefined) return 'NULL';
                if (typeof val === 'string') return `'${val.replace(/'/g, "''")}'`;
                return val;
            }).join(', ')})`
        ).join(',\n');

        const query = `
            INSERT INTO "${tableName}" (${columnList})
            VALUES ${values}
        `;

        await sql.unsafe(query);
    } catch (error) {
        console.error('Error copying batch:', error);
        throw error;
    }
}

if (isMainThread) {
    async function importExcelToPostgres(filePath: string, tableName: string): Promise<void> {
        console.time('Import Duration');
        const sql = createDbConnection();
        let totalProcessed = 0;
        
        try {
            console.log('Starting import process...');
            
            const workbook = XLSX.readFile(filePath, { sheetRows: 1 });
            const worksheet = workbook.Sheets[workbook.SheetNames[0]];
            const headers = XLSX.utils.sheet_to_json(worksheet, { header: 1 })[0] as string[];
            
            console.log('Creating table with columns:', headers);
            await createTable(sql, tableName, headers);
            
            // Create workers
            const workers = Array(NUM_WORKERS).fill(null).map(() => 
                new Worker(__filename, {
                    workerData: { tableName, columns: headers }
                })
            );

            let workerIndex = 0;
            
            // Process the file in batches
            await processExcelInBatches(filePath, async (rows) => {
                const worker = workers[workerIndex];
                worker.postMessage({ type: 'data', batch: rows });
                
                totalProcessed += rows.length;
                console.log(`Total rows processed: ${totalProcessed}`);
                
                workerIndex = (workerIndex + 1) % workers.length;
                
                // Wait for worker to complete
                await new Promise((resolve, reject) => {
                    worker.once('message', resolve);
                    worker.once('error', reject);
                });
            });

            // Clean up workers
            await Promise.all(workers.map(worker => 
                new Promise<void>((resolve) => {
                    worker.postMessage({ type: 'end' });
                    worker.on('exit', () => resolve());
                })
            ));

            console.log('Import completed successfully');
            console.timeEnd('Import Duration');

        } catch (error) {
            console.error('Error during import:', error);
            throw error;
        } finally {
            await sql.end();
        }
    }

    const filePath = path.resolve("./src/data.xls");
    const tableName = "excel_data";
    
    if (!fs.existsSync(filePath)) {
        console.error('Excel file not found:', filePath);
        process.exit(1);
    }

    importExcelToPostgres(filePath, tableName)
        .then(() => process.exit(0))
        .catch((error) => {
            console.error('Failed to import data:', error);
            process.exit(1);
        });

} else {
    // Worker thread code
    const sql = createDbConnection();
    
    parentPort?.on('message', async (message) => {
        try {
            if (message.type === 'data') {
                await copyBatchToPostgres(
                    sql, 
                    workerData.tableName, 
                    workerData.columns, 
                    message.batch
                );
                parentPort?.postMessage('done');
            } else if (message.type === 'end') {
                await sql.end();
                process.exit(0);
            }
        } catch (error) {
            console.error('Worker error:', error);
            parentPort?.postMessage('error');
            process.exit(1);
        }
    });
}