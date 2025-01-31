import time
import psycopg2
import io
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from dotenv import load_dotenv
import python_calamine
from typing import IO, Iterator
import pandas as pd

load_dotenv()

conn_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
file_path = os.getenv('EXCEL_FILE_PATH')
CHUNK_SIZE = int(os.getenv('CHUNKS_SIZE',500000))

NUMBER_OF_THREADS = os.cpu_count()-1

def iter_excel_calamine(file: IO[bytes]) -> Iterator[dict[str, object]]:
    print("Reading Excel file...")
    start_time=time.time()
    workbook = python_calamine.CalamineWorkbook.from_filelike(file)
    rows = iter(workbook.get_sheet_by_index(0).to_python())
    headers = list(map(str, next(rows)))
    print(f"Excel file read duration: {time.time() - start_time:.2f} seconds")
    for row in rows:
        yield dict(zip(headers, row))

def get_sql_type(value):
    if isinstance(value, int):
        return 'INTEGER'
    elif isinstance(value, float):
        return 'DOUBLE PRECISION'
    elif isinstance(value, (pd.Timestamp, pd.Timedelta)):
        return 'TIMESTAMP'
    else:
        return 'TEXT'

def process_chunk(chunk, table_name):
    try:
        with psycopg2.connect(conn_string) as pg_conn:
            with pg_conn.cursor() as cur:
                csv_buffer = io.StringIO()
                pd.DataFrame(chunk).to_csv(csv_buffer, index=False, header=False)
                csv_buffer.seek(0)
                cur.copy_from(csv_buffer, table_name, sep=',', null='')
                pg_conn.commit()
    except Exception as e:
        print(f"Error processing chunk: {str(e)}")
        raise

def main():
    start_time = time.time()
    try:
        table_name = 'copy_test'
        
        with open(file_path, 'rb') as file:
            excel_iterator = iter_excel_calamine(file)
            
            # Get the first row to infer column types
            first_row = next(excel_iterator)
            column_defs = [f'"{col}" {get_sql_type(val)}' for col, val in first_row.items()]
            column_defs_str = ',\n    '.join(column_defs)

            table_create_sql = f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                {column_defs_str}
            )
            '''

            with psycopg2.connect(conn_string) as pg_conn:
                with pg_conn.cursor() as cur:
                    cur.execute(table_create_sql)
                    cur.execute(f'TRUNCATE TABLE {table_name}')
                pg_conn.commit()

            chunk = [first_row]
            with ThreadPoolExecutor(max_workers=NUMBER_OF_THREADS) as executor:
                futures = []
                for row in tqdm(excel_iterator, desc="Processing rows"):
                    chunk.append(row)
                    if len(chunk) >= CHUNK_SIZE:
                        futures.append(executor.submit(process_chunk, chunk, table_name))
                        chunk = []

                if chunk:  # Process any remaining rows
                    futures.append(executor.submit(process_chunk, chunk, table_name))

                for future in as_completed(futures):
                    future.result()

        print(f"Total COPY duration: {time.time() - start_time:.2f} seconds")
    except Exception as e:
        import traceback
        print(f"Error during COPY operation: {str(e)}")
        print("Full traceback:")
        print(traceback.format_exc())

if __name__ == "__main__":
    main()