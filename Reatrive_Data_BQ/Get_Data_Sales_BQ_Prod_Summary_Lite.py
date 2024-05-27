import pandas_gbq
import pandas as pd
import numpy as np
from tqdm import tqdm
import time
import os
from tabulate import tabulate
from colorama import Fore
import getpass

class QueryExecutor_Summary_Lite:
    def __init__(self, project_id):
        self.project_id = project_id
        self.default_password = "kak_wida"
        self.password = None
        
    def get_password(self):
        if self.password is None:
            self.password = getpass.getpass("Enter your password: ")
        
    def check_and_install_libraries(self):
        libraries = ['pandas', 'numpy', 'tqdm', 'pandas_gbq', 'tabulate', 'colorama']
        for library in libraries:
            try:
                __import__(library)
                print(f"{library} library is already installed.")
                print('=' * 70)
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

    def run_queries_with_libraries_check(self):
        self.check_and_install_libraries()
        while True:
            print(" ")
            print(Fore.GREEN + "This Program was Built by @Vrooh933 by Using Python Programming Language\n")
            print(Fore.GREEN + "Program Started")
            print("=" * 70)
            print(Fore.GREEN + "Program Name : Get Data Sales Big Query Summary Bulanan")
            print("=" * 70)
            
            self.get_password()
            
            if self.password == self.default_password:
                print("Authentication successful!\n")
            else:
                print("Authentication failed. Please check your password.")
                break
            
            start_date = input("Enter Start Date Format YYYY-MM-DD:")
            end_date = input("Enter End Date Format YYYY-MM-DD:")
            result_df, result_df2 = self.run_queries(start_date, end_date)
            
            result_df_selected = result_df.iloc[:, 0:9]
            result_df2_selected = result_df2.iloc[:, 0:9]
            
            headers_df = result_df_selected.columns.tolist()
            headers_df2 = result_df2_selected.columns.tolist()
            
            print("\nResult Original DataFrame Preview:")
            print(tabulate(result_df_selected.head(32), headers_df, tablefmt='psql'))
            print("\nResult After Process DataFrame 2 Preview:")
            print(tabulate(result_df2_selected.head(32), headers_df2, tablefmt='psql'))
            print("\nWorking' finished !!!")
            run_again = input("Do you want to run the program again? (Y/N): ").upper()

            if run_again != 'Y':
                break

    def execute_query(self, sql_code):
        return pandas_gbq.read_gbq(sql_code, project_id=self.project_id)
    
    def append_rows(self, base_df):
        # crete mapping
        mapping = {
            "Takeaway_Charge":"Takeaway Cost",
            "Total_Payment":"Total Payment",
            "Total_Discount":"Discount",
            "Food_Gross_Sales":"Food Gross Sales",
            "BVRG_Gross_Sales":"Beverage Gross Sales",
            "Delivery_Cost_Total":"Delivery Cost",
            "Net_Sales":"Net Sales",
            "Total_Bill":"Total Bill",
            "TAX":"Restaurant Tax" 
        }

        base_df = base_df.rename(columns = mapping)
        
        desired_order = [
            'store_code','Total Bill','Food Gross Sales',
            'Beverage Gross Sales','Discount','Net Sales','Restaurant Tax',
            'Delivery Cost','Takeaway Cost','Total Payment']

        result_df = base_df.reindex(columns = desired_order)
        return result_df

    def save_to_csv(self, result_df2, start_date, end_date):
        file_name = f"monthly_SMK_SUMMARY_LITE {start_date.replace('-', '_')}_to_{end_date.replace('-', '_')}.csv"
        download_folder = os.path.expanduser("~/Downloads")
        file_path = os.path.join(download_folder, file_name)
        result_df2.to_csv(file_path, index=False)
        print(f"Dataframe berhasil disimpan di: {file_path}")
    
    def run_queries(self, start_date, end_date):
        # SQL Code 1
        sql_code_1 = f"""
        --raw data Bill
        SELECT
          store_code,
          COUNT(order_id) as Total_Bill,
          SUM(CASE WHEN segment = "Delivery" THEN delivery_fee ELSE 0 END) as Delivery_Cost_Total,
          SUM(CASE WHEN segment in ("Dine-in", "Takeaway", "Aggregator") THEN delivery_fee ELSE 0 END) as Takeaway_Charge,
          SUM(discount) as Total_Discount,
          SUM(net_sales) as Net_Sales,
          SUM(tax) as TAX,
          SUM(transaction_value_with_tax) as Total_Payment
        FROM
          `phi-gcp-2021.analytics.orders_staging`
        WHERE
          business_date between "{start_date}" and "{end_date}"
        GROUP BY
          store_code
        ORDER BY
          store_code asc;
        """
        df1 = self.execute_query(sql_code_1)

        # SQL Code 2
        sql_code_2 = f"""
        SELECT
          od.store_code,
          SUM(CASE WHEN od.product_category_name NOT IN ("Beverage", "Pasta Hut Drink", "Coffee Premium") THEN od.gross_sales ELSE 0 END) AS Food_Gross_Sales,
          SUM(CASE WHEN od.product_category_name IN ("Beverage", "Pasta Hut Drink", "Coffee Premium") THEN od.gross_sales ELSE 0 END) AS BVRG_Gross_Sales,
        FROM
          `phi-gcp-2021.analytics.order_details_staging` as od
        LEFT JOIN
          `phi-gcp-2021.analytics.orders_staging` as o
        ON
          od.order_id = o.order_id
        WHERE
          od.business_date between "{start_date}" and "{end_date}"
        GROUP BY
          od.store_code
        ORDER BY
          od.store_code asc;
        """
        df2 = self.execute_query(sql_code_2)
        print("Merging... :)")
        print("="*70)
        
        for _ in tqdm(range(len(df1)), desc="Merging Progress", unit="row"):
            time.sleep(0.01)

        result_df = pd.merge(df1, df2, on=['store_code'])
        
        for _ in tqdm(range(20), desc="Merging Progress", unit="second"):
            time.sleep(0.01)
        print("="*70)
        print("Finished !!!")
        
        result_df_copy = result_df.copy()
        result_df2 = self.append_rows(result_df_copy)
        self.save_to_csv(result_df2, start_date, end_date)

        return result_df, result_df2

project_id = 'phi-gcp-2021'
query_executor = QueryExecutor_Summary_Lite(project_id)
query_executor.run_queries_with_libraries_check()


# In[ ]:




