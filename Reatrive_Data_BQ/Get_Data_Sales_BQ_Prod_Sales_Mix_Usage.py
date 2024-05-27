import pandas_gbq
import pandas as pd
import numpy as np
from tqdm import tqdm
import time
import os
from tabulate import tabulate
from colorama import Fore
import getpass

class QueryExecutor_Sales_Mix_Usage:
    def __init__(self, project_id):
        self.project_id = project_id
        self.default_password = "kak_wida"
        self.password = None
        
    def get_password(self):
        if self.password is None:
            self.password = getpass.getpass("Enter your password: ")
        
    def check_and_install_libraries(self):
        libraries = ['pandas', 'numpy', 'tqdm', 'pandas_gbq', 'tabulate', 'colorama']
        print("REQUIRED LIBRARY")
        print("="*30)
        for library in libraries:
            try:
                __import__(library)
                print(f"{library} library is already installed.")
            except ModuleNotFoundError:
                install_library = input(f"{library} library is not installed. Do you want to install it now? (Y/N): ").upper()
                if install_library == 'Y':
                    if library == 'pandas_gbq':
                        os.system("pip install pandas-gbq")
                    else:
                        os.system(f"pip install {library}")
                else:
                    print(f"{library} is required for the program. Exiting.")
                    exit()
        print("="*30)

    def run_queries_with_libraries_check(self):
        self.check_and_install_libraries()
        while True:
            print(" ")
            print(Fore.GREEN + "This Program was Built by @Vrooh933 by Using Python Programming Language\n")
            print(Fore.GREEN + "Program Started")
            print("=" * 70)
            print(Fore.GREEN + "Program Name : Inventory Sales Mix Usage")
            print("=" * 70)
            
            self.get_password()
            
            if self.password == self.default_password:
                print("Authentication successful!\n")
            else:
                print("Authentication failed. Please check your password.")
                break
            
            start_date = input("Enter Start Date Format YYYY-MM-DD: ")
            end_date = input("Enter End Date Format YYYY-MM-DD: ")
            
            result_df = self.run_queries(start_date, end_date)
            result_df_selected = result_df.iloc[:, 0:8]
            headers_df = result_df_selected.columns.tolist()
            
            print("\nResult Original DataFrame Preview:")
            print(tabulate(result_df_selected.head(32), headers_df, tablefmt='psql'))
            print("\nWorking' finished !!!")
            run_again = input("Do you want to run the program again? (Y/N): ").upper()

            if run_again != 'Y':
                break

    def execute_query(self, sql_code):
        return pandas_gbq.read_gbq(sql_code, project_id=self.project_id)

    def save_to_csv(self, result_df, start_date, end_date):
        file_name = f"Sales Mix Usage {start_date.replace('-', '_')}_to_{end_date.replace('-', '_')}.csv"
        download_folder = os.path.expanduser("~/Downloads")
        file_path = os.path.join(download_folder, file_name)
        result_df.to_csv(file_path, index=False)
        print(f"Dataframe berhasil disimpan di: {file_path}")
    
    def run_queries(self, start_date, end_date):
        # SQL Code 1
        sql_code_1 = f"""
        SELECT
        Quantity.store_code,
        Quantity.sku,
        Quantity.sku_name,
        Quantity.Usage_Sales_QTY,
        total_potensial.Total_Potensial,
        round((total_potensial.Total_Potensial / Quantity.Usage_Sales_QTY),0) AS Potensial
        FROM 
        (SELECT
            store_code,
            sku,
            sku_name,
            SUM(quantity) AS Usage_Sales_QTY
        FROM (
            SELECT DISTINCT
            store_code,
            order_code,
            order_detail_id,
            sku,
            sku_name,
            quantity
            FROM
            `phi-gcp-2021.analytics.inventory_material_usage_report_staging`
            where business_date between "{start_date}" and "{end_date}"
        ) AS distinct_sales
        GROUP BY
            sku, sku_name, store_code
        ORDER BY
            store_code, sku_name asc ) AS Quantity
        JOIN
        (SELECT
            store_code,
            sku,
            sku_name,
            ROUND(SUM(total), 0) AS Total_Potensial
        FROM
            `phi-gcp-2021.analytics.inventory_material_usage_report_staging`
        WHERE
            business_date BETWEEN "{start_date}" AND "{end_date}"
        GROUP BY 
            store_code, sku, sku_name
        ORDER BY 
            store_code, sku asc) as total_potensial
        ON
        Quantity.store_code = total_potensial.store_code
        AND Quantity.sku = total_potensial.sku
        ORDER BY
        Quantity.store_code, Quantity.sku ASC;
          """
        df1 = self.execute_query(sql_code_1)

        print("Merging... :)")
        print("="*70)
        
        for _ in tqdm(range(len(df1)), desc="Merging Progress", unit="row"):
            time.sleep(0.01)

        for _ in tqdm(range(20), desc="Merging Progress", unit="second"):
            time.sleep(0.01)
        print("="*70)
        print("Finished !!!")
        
        result_df = df1.copy()
        self.save_to_csv(result_df, start_date, end_date)
        return result_df

project_id = 'phi-gcp-2021'
query_executor = QueryExecutor_Sales_Mix_Usage(project_id)
query_executor.run_queries_with_libraries_check()



