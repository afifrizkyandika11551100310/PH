import pandas_gbq
import pandas as pd
import numpy as np
from tqdm import tqdm
import time
import os
from tabulate import tabulate
from colorama import Fore
import getpass

class QueryExecutor_HistoryBill_PerOutlet:
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
            print(Fore.GREEN + "Program Name : Get Data Bill Payment Per-Outlet by Date")
            print("=" * 70)
            
            self.get_password()
            
            if self.password == self.default_password:
                print("Authentication successful!\n")
            else:
                print("Authentication failed. Please check your password.")
                break
            
            store_code = input("Enter Store Code (Contoh : B001):")
            start_date = input("Enter Start Date Format YYYY-MM-DD:")
            end_date = input("Enter End Date Format YYYY-MM-DD:")
            result_df = self.run_queries(start_date, end_date, store_code)
            
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

    def save_to_csv(self, result_df2, start_date, end_date, store_code):
        file_name = f"monthly DATA History Bill Store Code {store_code} {start_date.replace('-', '_')}_to_{end_date.replace('-', '_')}.csv"
        download_folder = os.path.expanduser("~/Downloads")
        file_path = os.path.join(download_folder, file_name)
        result_df2.to_csv(file_path, index=False)
        print(f"Dataframe berhasil disimpan di: {file_path}")
    
    def run_queries(self, start_date, end_date, store_code):
        # SQL Code 1
        sql_code_1 = f"""
        select
          o.store_code,
          o.store_name,
          o.business_date,
          o.order_number,
          o.segment,
          o.product_total_cost,
          o.tax,
          o.delivery_fee,
          o.net_sales,
          o.transaction_value_with_tax as payment_value,
          o.discount
        from
          `phi-gcp-2021.analytics.orders_staging` as o
        where
          o.business_date between "{start_date}" and "{end_date}"
          and o.store_code = "{store_code}"
        order by 
          o.store_code,
          o.business_date,
          o.order_number asc;
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
        self.save_to_csv(result_df, start_date, end_date, store_code)
        return result_df

project_id = 'phi-gcp-2021'
query_executor = QueryExecutor_HistoryBill_PerOutlet(project_id)
query_executor.run_queries_with_libraries_check()



