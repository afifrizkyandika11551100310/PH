import pandas_gbq
import pandas as pd
import numpy as np
from tqdm import tqdm
import time
import os
from tabulate import tabulate
from colorama import Fore, Style
import getpass

# Class get data sales summary
class QueryExecutor_summary:
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
                print("REQUIRED LIBRARY")
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

    def run_queries_with_libraries_check(self):
        self.check_and_install_libraries()
        while True:
            print(" ")
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
            
            start_date = input("Enter Start Date Format YYYY-MM-DD: ")
            end_date = input("Enter End Date Format YYYY-MM-DD: ")
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
            return run_again

    def execute_query(self, sql_code):
        return pandas_gbq.read_gbq(sql_code, project_id=self.project_id)
    
    def append_rows(self, base_df, start_date, end_date):
        base_df["business_date"] = f"{start_date.replace('-', '-')} to {end_date.replace('-', '-')}"
        base_df['Harian / Bulanan'] = "1 Bulan"
        base_df['Nama_File'] = "Vrooh933.csv"
        
        # crete mapping
        mapping = {
        'Bill_Eat_In': 'BILL EAT IN',
        'Bill_Dine_In': 'BILL DINE IN',
        'Bill_Takeaway': 'BILL TAKE AWAY',
        'Bill_Delivery': 'BILL DELIVERY',
        'Bill_Aggregator': 'BILL AGGREGATOR',
        'Food_Gross_Sales_Eat_In': 'FOOD GROSS SALES EAT IN',
        'Food_Gross_Sales_Dine_In': 'FOOD GROSS SALES DINE IN',
        'Food_Gross_Sales_Takeaway': 'FOOD GROSS SALES TAKE AWAY',
        'Food_Gross_Sales_Delivery': 'FOOD GROSS SALES DELIVERY',
        'Food_Gross_Sales_Aggregator': 'FOOD GROSS SALES AGGREGATOR',
        'BVRG_Gross_Sales_Eat_In': 'BVRG GROSS SALES EAT IN',
        'BVRG_Gross_Sales_Dine_In': 'BVRG GROSS SALES DINE IN',
        'BVRG_Gross_Sales_Takeaway': 'BVRG GROSS SALES TAKE AWAY',
        'BVRG_Gross_Sales_Delivery': 'BVRG GROSS SALES DELIVERY',
        'BVRG_Gross_Sales_Aggregator': 'BVRG GROSS SALES AGGREGATOR',
        'Discount_Food_Eat_In': 'DISCOUNT FOOD EAT IN',
        'Discount_Food_Dine_In': 'DISCOUNT FOOD DINE IN',
        'Discount_Food_Takeaway': 'DISCOUNT FOOD TAKE AWAY',
        'Discount_Food_Delivery': 'DISCOUNT FOOD DELIVERY',
        'Discount_Food_Aggregator': 'DISCOUNT FOOD AGGREGATOR',
        'Discount_BVRG_Eat_In': 'DISCOUNT BVRG EAT IN',
        'Discount_BVRG_Dine_In': 'DISCOUNT BVRG DINE IN',
        'Discount_BVRG_Takeaway': 'DISCOUNT BVRG TAKE AWAY',
        'Discount_BVRG_Delivery': 'DISCOUNT BVRG DELIVERY',
        'Discount_BVRG_Aggregator': 'DISCOUNT BVRG AGGREGATOR',
        'Delivery_Cost_Total': 'DELIVERY COST TOTAL',
        'Takaway_Cost_Eat_In': 'TAKEAWAY COST EAT IN',
        'Takeaway_Cost_Dine_In': 'TAKEAWAY COST DINE IN',
        'Takeaway_Cost_Take_Away': 'TAKEAWAY COST TAKE AWAY',
        'Takeaway_Cost_Aggregator': 'TAKEAWAY COST AGGREGATOR',
        'Restaurant_Tax_Eat_In': 'RESTAURANT TAX EAT IN',
        'Restaurant_Tax_Dine_In': 'RESTAURANT TAX DINE IN',
        'Restaurant_Tax_Takeaway': 'RESTAURANT TAX TAKE AWAY',
        'Restaurant_Tax_Delivery': 'RESTAURANT TAX DELIVERY',
        'Restaurant_Tax_Aggregator': 'RESTAURANT TAX AGGREGATOR',
        'Cash': 'Cash',
        'IM_GRAB': 'IM GRAB',
        'GO_BIZ': 'GO-BIZ',
        'BCA_VA': 'BCA VA',
        'BCA_EDC_DBT': 'BCA EDC DBT',
        'OVO_ONLINE': 'OVO ONLINE',
        'BNI_EDC_VM': 'BNI EDC VM',
        'REIMBURSE_IM_GRAB': 'REIMBURSE IM GRAB',
        'OTHER_PAYMENT': 'Other Payment',
        'BCA_EDCV_VM': 'BCA EDCV VM',
        'REIMBURSE_GOBIZ': 'REIMBURSE GOBIZ',
        'MANDIRI_VMD': 'MANDIRI VMD',
        'CIMB_QRIS': 'CIMB QRIS',
        'GO_PAY_ONLINE': 'GO-PAY ONLINE',
        'BCA_EDC_BCA': 'BCA EDC BCA',
        'BCA_RWD': 'BCA RWD',
        'BRI_VM': 'BRI V/M',
        'AMEX': 'AMEX',
        'OVO': 'OVO',
        'VM_ONLINE': 'V/M ONLINE',
        'MANDIRI_QR_LIVIN': 'MANDIRI QR LIVIN',
        'GOPAY_ONLINE': 'GOPAY ONLINE',
        'SHOPEEPAY_ONLINE': 'SHOPEEPAY ONLINE',
        'SHOPEEFOOD': 'SHOPEEFOOD',
        'GOPAY': 'GO-PAY',
        'LIVIN_MANDIRI': 'Livin MANDIRI',
        'SHOPEE_ONLINE': 'SHOPEE ONLINE',
        'PASTAHUT_IMGRAB': 'PASTAHUT IMGRAB',
        'VOUCHER_PH': 'Voucher PH',
        'V_M_ONLINE': 'VM Online',
        'SHOPEE_QRIS': 'SHOPEE QRIS',
        'SHOPEE_PAY': 'SHOPEE PAY',
        'PASTAHUT_GOBIZ': 'PASTAHUT GOBIZ',
        'P_POINT_MANDIRI': 'P Point Mandiri',
        'REIMBURSE_SHOPEE': 'REIMBURSE SHOPEE',
        'BNI_POINT': 'BNI Point',
        'BCA_FLAZZ': 'BCA Flazz',
        'CC_DEBIT_ONLINE': 'Cc/Debit Online',
        'LINK_AJA_MANDIRI': 'LINK AJA (Mandiri)',
        'DANA_BALANCE': 'DANA BALANCE',
        'VOUCHER_DIGITAL': 'Voucher Digital',
        'DANA_KREDIT': 'DANA KREDIT',
        'DG_VOUCHER': 'DG Voucher',
        'BCA_QRIS': 'BCA QRIS',
        'BTN': 'BTN',
        'KREDIVO': 'Kredivo',
        'VOUCHER_SODEXO_PLUXEE': 'Voucher Sodexo/Pluxee',
        'INDODANA':'Indodana',
        }

        base_df = base_df.rename(columns = mapping)
        
        desired_order = [
            'store_code', 'business_date', 'BILL EAT IN', 'BILL DINE IN',
            'BILL TAKE AWAY', 'BILL DELIVERY', 'BILL AGGREGATOR',
            'FOOD GROSS SALES EAT IN', 'FOOD GROSS SALES DINE IN',
            'FOOD GROSS SALES TAKE AWAY', 'FOOD GROSS SALES DELIVERY',
            'FOOD GROSS SALES AGGREGATOR', 'BVRG GROSS SALES EAT IN',
            'BVRG GROSS SALES DINE IN', 'BVRG GROSS SALES TAKE AWAY',
            'BVRG GROSS SALES DELIVERY', 'BVRG GROSS SALES AGGREGATOR',
            'DISCOUNT FOOD EAT IN', 'DISCOUNT FOOD DINE IN',
            'DISCOUNT FOOD TAKE AWAY', 'DISCOUNT FOOD DELIVERY',
            'DISCOUNT FOOD AGGREGATOR', 'DISCOUNT BVRG EAT IN',
            'DISCOUNT BVRG DINE IN', 'DISCOUNT BVRG TAKE AWAY',
            'DISCOUNT BVRG DELIVERY', 'DISCOUNT BVRG AGGREGATOR',
            'DELIVERY COST TOTAL', 'TAKEAWAY COST EAT IN', 'TAKEAWAY COST DINE IN', 'TAKEAWAY COST TAKE AWAY', 'TAKEAWAY COST AGGREGATOR',
            'RESTAURANT TAX EAT IN', 'RESTAURANT TAX DINE IN', 'RESTAURANT TAX TAKE AWAY',
            'RESTAURANT TAX DELIVERY', 'RESTAURANT TAX AGGREGATOR',
            'Cash', 'IM GRAB', 'GO-BIZ', 'BCA VA', 'BCA EDC DBT', 'OVO ONLINE',
            'BNI EDC VM', 'REIMBURSE IM GRAB', 'Other Payment', 'BCA EDCV VM',
            'REIMBURSE GOBIZ', 'MANDIRI VMD', 'CIMB QRIS', 'GO-PAY ONLINE',
            'BCA EDC BCA', 'BCA RWD', 'BRI V/M', 'AMEX', 'OVO', 'V/M ONLINE',
            'MANDIRI QR LIVIN', 'GOPAY ONLINE', 'SHOPEEPAY ONLINE', 'SHOPEEFOOD',
            'GO-PAY', 'Livin MANDIRI', 'SHOPEE ONLINE', 'PASTAHUT IMGRAB',
            'Voucher PH', 'VM Online', 'SHOPEE QRIS', 'SHOPEE PAY',
            'PASTAHUT GOBIZ', 'P Point Mandiri', 'REIMBURSE SHOPEE', 'BNI Point',
            'BCA Flazz', 'Cc/Debit Online', 'LINK AJA (Mandiri)', 'DANA BALANCE',
            'Voucher Digital', 'DANA KREDIT', 'DG Voucher', 'BCA QRIS', 'BTN',
            'Kredivo', 'Voucher Sodexo/Pluxee', 'Indodana', "Harian / Bulanan", "Nama_File",]

        result_df = base_df.reindex(columns = desired_order)
        return result_df

    def save_to_csv(self, result_df2, start_date, end_date):
        file_name = f"monthly_SMK_SUMMARY {start_date.replace('-', '_')}_to_{end_date.replace('-', '_')}.csv"
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
          --total_bill--
          0 as Bill_Eat_In,
          COUNT(CASE WHEN segment = "Dine-in" THEN order_id END) AS Bill_Dine_In,
          COUNT(CASE WHEN segment = "Takeaway" THEN order_id END) AS Bill_Takeaway,
          COUNT(CASE WHEN segment = "Delivery" THEN order_id END) as Bill_Delivery,
          COUNT(CASE WHEN segment = "Aggregator" THEN order_id END) as Bill_Aggregator,

          --total discount--
          0 as Discount_Food_Eat_In,
          SUM(CASE WHEN segment = "Dine-in" THEN discount ELSE 0 END) as Discount_Food_Dine_In,
          SUM(CASE WHEN segment = "Takeaway" THEN discount ELSE 0 END) as Discount_Food_Takeaway,
          SUM(CASE WHEN segment = "Delivery" THEN discount ELSE 0 END) as Discount_Food_Delivery,
          SUM(CASE WHEN segment = "Aggregator" THEN discount ELSE 0 END) as Discount_Food_Aggregator,
          0 as Discount_BVRG_Eat_In,
          0 as Discount_BVRG_Dine_In,
          0 as Discount_BVRG_Takeaway,
          0 as Discount_BVRG_Delivery,
          0 as Discount_BVRG_Aggregator,

          --total delfee--
          SUM(CASE WHEN segment = "Delivery" THEN delivery_fee ELSE 0 END) as Delivery_Cost_Total,
          
          --total takeaway charge--
          0 as Takaway_Cost_Eat_In,
          SUM(CASE WHEN segment = "Dine-in" THEN delivery_fee ELSE 0 END) as Takeaway_Cost_Dine_In,
          SUM(CASE WHEN segment = "Takeaway" THEN delivery_fee ELSE 0 END) as Takeaway_Cost_Take_Away,
          SUM(CASE WHEN segment = "Aggregator" THEN delivery_fee ELSE 0 END) as Takeaway_Cost_Aggregator,

          --total tax--
          0 as Restaurant_Tax_Eat_In,
          SUM(CASE WHEN segment = 'Dine-in' THEN tax ELSE 0 END) as Restaurant_Tax_Dine_In,
          SUM(CASE WHEN segment = 'Takeaway' THEN tax ELSE 0 END) as Restaurant_Tax_Takeaway,
          SUM(CASE WHEN segment = 'Delivery' THEN tax ELSE 0 END) as Restaurant_Tax_Delivery,
          SUM(CASE WHEN segment = 'Aggregator' THEN tax ELSE 0 END) as Restaurant_Tax_Aggregator,
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
          --food gross sales--
          0 as Food_Gross_Sales_Eat_In,
          SUM(CASE WHEN od.product_category_name NOT IN ("Beverage", "Pasta Hut Drink", "Coffee Premium") and o.segment = "Dine-in" THEN od.gross_sales ELSE 0 END) AS Food_Gross_Sales_Dine_In,
          SUM(CASE WHEN od.product_category_name NOT IN ("Beverage", "Pasta Hut Drink", "Coffee Premium") and o.segment = "Takeaway" THEN od.gross_sales ELSE 0 END) AS Food_Gross_Sales_Takeaway,
          SUM(CASE WHEN od.product_category_name NOT IN ("Beverage", "Pasta Hut Drink", "Coffee Premium") and o.segment = "Delivery" THEN od.gross_sales ELSE 0 END) AS Food_Gross_Sales_Delivery,
          SUM(CASE WHEN od.product_category_name NOT IN ("Beverage", "Pasta Hut Drink", "Coffee Premium") and o.segment = "Aggregator" THEN od.gross_sales ELSE 0 END) AS Food_Gross_Sales_Aggregator,

          --bvrg gross sales--
          0 as BVRG_Gross_Sales_Eat_In,
          SUM(CASE WHEN od.product_category_name IN ("Beverage", "Pasta Hut Drink", "Coffee Premium") and o.segment = "Dine-in" THEN od.gross_sales ELSE 0 END) AS BVRG_Gross_Sales_Dine_In,
          SUM(CASE WHEN od.product_category_name IN ("Beverage", "Pasta Hut Drink", "Coffee Premium") and o.segment = "Takeaway" THEN od.gross_sales ELSE 0 END) AS BVRG_Gross_Sales_Takeaway,
          SUM(CASE WHEN od.product_category_name IN ("Beverage", "Pasta Hut Drink", "Coffee Premium") and o.segment = "Delivery" THEN od.gross_sales ELSE 0 END) AS BVRG_Gross_Sales_Delivery,
          SUM(CASE WHEN od.product_category_name IN ("Beverage", "Pasta Hut Drink", "Coffee Premium") and o.segment = "Aggregator" THEN od.gross_sales ELSE 0 END) AS BVRG_Gross_Sales_Aggregator,
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

        # SQL Code 3
        sql_code_3 = f"""
        SELECT
          store_code,
          --payment_code--
          SUM(CASE WHEN payment_code = 1 THEN payment_value ELSE 0 END) AS Cash,
          SUM(CASE WHEN payment_code = 3 THEN payment_value ELSE 0 END) AS BCA_EDCV_VM,
          SUM(CASE WHEN payment_code = 4 THEN payment_value ELSE 0 END) AS BNI_EDC_VM,
          SUM(CASE WHEN payment_code = 5 THEN payment_value ELSE 0 END) AS BCA_EDC_BCA,

          SUM(CASE WHEN payment_code = 6 THEN payment_value ELSE 0 END) as BCA_FLAZZ,
          SUM(CASE WHEN payment_code = 7 THEN payment_value ELSE 0 END) as OVO,
          SUM(CASE WHEN payment_code = 8 THEN payment_value ELSE 0 END) as MANDIRI_VMD,
          SUM(CASE WHEN payment_code = 9 THEN payment_value ELSE 0 END) as VOUCHER_PH,

          SUM(CASE WHEN payment_code = 10 THEN payment_value ELSE 0 END) as AMEX,
          SUM(CASE WHEN payment_code = 13 THEN payment_value ELSE 0 END) as BCA_EDC_DBT,
          SUM(CASE WHEN payment_code = 14 THEN payment_value ELSE 0 END) as BCA_RWD,
          SUM(CASE WHEN payment_code = 16 THEN payment_value ELSE 0 END) as VOUCHER_DIGITAL,

          SUM(CASE WHEN payment_code = 18 THEN payment_value ELSE 0 END) as OTHER_PAYMENT,
          SUM(CASE WHEN payment_code = 22 THEN payment_value ELSE 0 END) as CC_DEBIT_ONLINE,
          SUM(CASE WHEN payment_code = 26 THEN payment_value ELSE 0 END) as VM_ONLINE,
          SUM(CASE WHEN payment_code = 27 THEN payment_value ELSE 0 END) as BCA_VA,

          SUM(CASE WHEN payment_code = 32 THEN payment_value ELSE 0 END) as BNI_POINT,
          SUM(CASE WHEN payment_code = 33 THEN payment_value ELSE 0 END) as LIVIN_MANDIRI,
          SUM(CASE WHEN payment_code = 34 THEN payment_value ELSE 0 END) as DANA_BALANCE,
          SUM(CASE WHEN payment_code = 35 THEN payment_value ELSE 0 END) as DANA_KREDIT,

          SUM(CASE WHEN payment_code = 37 THEN payment_value ELSE 0 END) as LINK_AJA_MANDIRI,
          SUM(CASE WHEN payment_code = 40 THEN payment_value ELSE 0 END) as GOPAY_ONLINE,
          SUM(CASE WHEN payment_code = 43 THEN payment_value ELSE 0 END) as BRI_VM,
          SUM(CASE WHEN payment_code = 47 THEN payment_value ELSE 0 END) as DG_VOUCHER,

          SUM(CASE WHEN payment_code = 50 THEN payment_value ELSE 0 END) as GOPAY,
          SUM(CASE WHEN payment_code = 51 THEN payment_value ELSE 0 END) as GO_BIZ,
          SUM(CASE WHEN payment_code = 52 THEN payment_value ELSE 0 END) as IM_GRAB,
          SUM(CASE WHEN payment_code = 53 THEN payment_value ELSE 0 END) as SHOPEE_PAY,

          SUM(CASE WHEN payment_code = 54 THEN payment_value ELSE 0 END) as SHOPEEPAY_ONLINE,
          SUM(CASE WHEN payment_code = 55 THEN payment_value ELSE 0 END) as SHOPEE_QRIS,
          SUM(CASE WHEN payment_code = 76 THEN payment_value ELSE 0 END) as REIMBURSE_GOBIZ,
          SUM(CASE WHEN payment_code = 77 THEN payment_value ELSE 0 END) as REIMBURSE_IM_GRAB,

          SUM(CASE WHEN payment_code = 78 THEN payment_value ELSE 0 END) as REIMBURSE_SHOPEE,
          SUM(CASE WHEN payment_code = 79 THEN payment_value ELSE 0 END) as OVO_ONLINE,
          SUM(CASE WHEN payment_code = 80 THEN payment_value ELSE 0 END) as MANDIRI_QR_LIVIN,
          SUM(CASE WHEN payment_code = 81 THEN payment_value ELSE 0 END) as PASTAHUT_IMGRAB,

          SUM(CASE WHEN payment_code = 89 THEN payment_value ELSE 0 END) as PASTAHUT_GOBIZ,
          SUM(CASE WHEN payment_code = 100 THEN payment_value ELSE 0 END) as SHOPEEFOOD, 
          SUM(CASE WHEN payment_code = 101 THEN payment_value ELSE 0 END) as CIMB_QRIS,
          SUM(CASE WHEN payment_code = 102 THEN payment_value ELSE 0 END) as BCA_QRIS,
          0 as GO_PAY_ONLINE,
          0 as SHOPEE_ONLINE,
          0 as V_M_ONLINE,
          0 as P_POINT_MANDIRI,

          SUM(CASE WHEN payment_code = 103 THEN payment_value ELSE 0 END) as KREDIVO,
          SUM(CASE WHEN payment_code = 104 THEN payment_value ELSE 0 END) as BTN,
          SUM(CASE WHEN payment_code = 105 THEN payment_value ELSE 0 END) as VOUCHER_SODEXO_PLUXEE,
          SUM(CASE WHEN payment_code = 106 THEN payment_value ELSE 0 END) as INDODANA,
        FROM 
          `phi-gcp-2021.analytics.order_payment_staging`
        WHERE
          business_date between "{start_date}" and "{end_date}"
        GROUP BY 
          store_code
        ORDER BY
          store_code asc;
        """
        df3 = self.execute_query(sql_code_3)
        
        print("Merging... :)")
        print("="*70)
        
        for _ in tqdm(range(len(df1)), desc="Merging Progress", unit="row"):
            time.sleep(0.01)

        result_df = pd.merge(pd.merge(df1,df2, on=['store_code']),df3,on= ['store_code'])
        
        for _ in tqdm(range(20), desc="Merging Progress", unit="second"):
            time.sleep(0.01)
        print("="*70)
        print("Finished !!!")
        
        result_df_copy = result_df.copy()
        result_df2 = self.append_rows(result_df_copy, start_date, end_date)
        self.save_to_csv(result_df2, start_date, end_date)
        return result_df, result_df2

# class get data sales harian
class QueryExecutor_harian:
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
                print("REQUIRED LIBRARY")
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

    def run_queries_with_libraries_check(self):
        self.check_and_install_libraries()
        while True:
            print(" ")
            print(Fore.GREEN + "Program Started")
            print("=" * 70)
            print(Fore.GREEN + "Program Name : Get Data Sales Big Query Summary Harian")
            print("=" * 70)
            
            self.get_password()
            
            if self.password == self.default_password:
                print("Authentication successful!\n")
            else:
                print("Authentication failed. Please check your password.")
                break
            
            start_date = input("Enter Start Date Format YYYY-MM-DD: ")
            end_date = input("Enter End Date Format YYYY-MM-DD: ")
            result_df, result_df2 = self.run_queries(start_date, end_date)
            
            result_df_selected = result_df.iloc[:, 0:9]
            result_df2_selected = result_df2.iloc[:, 0:9]
            
            headers_df = result_df_selected.columns.tolist()
            headers_df2 = result_df2_selected.columns.tolist()
            
            print("\nResult Original DataFrame Preview:")
            print(tabulate(result_df_selected.head(32), headers_df, tablefmt='psql'))
            print("\nResult After Process DataFrame 2 Preview:")
            print(tabulate(result_df2_selected.head(32), headers_df2, tablefmt='psql'))
            print("\nWorking finished !!!")
            run_again = input("Do you want to run the program again? (Y/N): ").upper()                
            return run_again

    def execute_query(self, sql_code):
        return pandas_gbq.read_gbq(sql_code, project_id=self.project_id)
    
    def append_rows(self, base_df):
        base_df['New_Column'] = base_df['business_date'].isnull().astype(int)
        base_df = base_df.sort_values(by=['store_code', 'New_Column', 'business_date'], ascending=[True, True, True])
        base_df['Harian / Bulanan'] = np.where(base_df['New_Column'] == 0, '1 Hari', '1 Bulan')
        base_df['Nama_File'] = "Vrooh933.csv"
        base_df = base_df.drop(columns=['New_Column'])
        
        # crete mapping
        mapping = {
        'Bill_Eat_In': 'BILL EAT IN',
        'Bill_Dine_In': 'BILL DINE IN',
        'Bill_Takeaway': 'BILL TAKE AWAY',
        'Bill_Delivery': 'BILL DELIVERY',
        'Bill_Aggregator': 'BILL AGGREGATOR',
        'Food_Gross_Sales_Eat_In': 'FOOD GROSS SALES EAT IN',
        'Food_Gross_Sales_Dine_In': 'FOOD GROSS SALES DINE IN',
        'Food_Gross_Sales_Takeaway': 'FOOD GROSS SALES TAKE AWAY',
        'Food_Gross_Sales_Delivery': 'FOOD GROSS SALES DELIVERY',
        'Food_Gross_Sales_Aggregator': 'FOOD GROSS SALES AGGREGATOR',
        'BVRG_Gross_Sales_Eat_In': 'BVRG GROSS SALES EAT IN',
        'BVRG_Gross_Sales_Dine_In': 'BVRG GROSS SALES DINE IN',
        'BVRG_Gross_Sales_Takeaway': 'BVRG GROSS SALES TAKE AWAY',
        'BVRG_Gross_Sales_Delivery': 'BVRG GROSS SALES DELIVERY',
        'BVRG_Gross_Sales_Aggregator': 'BVRG GROSS SALES AGGREGATOR',
        'Discount_Food_Eat_In': 'DISCOUNT FOOD EAT IN',
        'Discount_Food_Dine_In': 'DISCOUNT FOOD DINE IN',
        'Discount_Food_Takeaway': 'DISCOUNT FOOD TAKE AWAY',
        'Discount_Food_Delivery': 'DISCOUNT FOOD DELIVERY',
        'Discount_Food_Aggregator': 'DISCOUNT FOOD AGGREGATOR',
        'Discount_BVRG_Eat_In': 'DISCOUNT BVRG EAT IN',
        'Discount_BVRG_Dine_In': 'DISCOUNT BVRG DINE IN',
        'Discount_BVRG_Takeaway': 'DISCOUNT BVRG TAKE AWAY',
        'Discount_BVRG_Delivery': 'DISCOUNT BVRG DELIVERY',
        'Discount_BVRG_Aggregator': 'DISCOUNT BVRG AGGREGATOR',
        'Delivery_Cost_Total': 'DELIVERY COST TOTAL',
        'Takaway_Cost_Eat_In': 'TAKEAWAY COST EAT IN',
        'Takeaway_Cost_Dine_In': 'TAKEAWAY COST DINE IN',
        'Takeaway_Cost_Take_Away': 'TAKEAWAY COST TAKE AWAY',
        'Takeaway_Cost_Aggregator': 'TAKEAWAY COST AGGREGATOR',
        'Restaurant_Tax_Eat_In': 'RESTAURANT TAX EAT IN',
        'Restaurant_Tax_Dine_In': 'RESTAURANT TAX DINE IN',
        'Restaurant_Tax_Takeaway': 'RESTAURANT TAX TAKE AWAY',
        'Restaurant_Tax_Delivery': 'RESTAURANT TAX DELIVERY',
        'Restaurant_Tax_Aggregator': 'RESTAURANT TAX AGGREGATOR',
        'Cash': 'Cash',
        'IM_GRAB': 'IM GRAB',
        'GO_BIZ': 'GO-BIZ',
        'BCA_VA': 'BCA VA',
        'BCA_EDC_DBT': 'BCA EDC DBT',
        'OVO_ONLINE': 'OVO ONLINE',
        'BNI_EDC_VM': 'BNI EDC VM',
        'REIMBURSE_IM_GRAB': 'REIMBURSE IM GRAB',
        'OTHER_PAYMENT': 'Other Payment',
        'BCA_EDCV_VM': 'BCA EDCV VM',
        'REIMBURSE_GOBIZ': 'REIMBURSE GOBIZ',
        'MANDIRI_VMD': 'MANDIRI VMD',
        'CIMB_QRIS': 'CIMB QRIS',
        'GO_PAY_ONLINE': 'GO-PAY ONLINE',
        'BCA_EDC_BCA': 'BCA EDC BCA',
        'BCA_RWD': 'BCA RWD',
        'BRI_VM': 'BRI V/M',
        'AMEX': 'AMEX',
        'OVO': 'OVO',
        'VM_ONLINE': 'V/M ONLINE',
        'MANDIRI_QR_LIVIN': 'MANDIRI QR LIVIN',
        'GOPAY_ONLINE': 'GOPAY ONLINE',
        'SHOPEEPAY_ONLINE': 'SHOPEEPAY ONLINE',
        'SHOPEEFOOD': 'SHOPEEFOOD',
        'GOPAY': 'GO-PAY',
        'LIVIN_MANDIRI': 'Livin MANDIRI',
        'SHOPEE_ONLINE': 'SHOPEE ONLINE',
        'PASTAHUT_IMGRAB': 'PASTAHUT IMGRAB',
        'VOUCHER_PH': 'Voucher PH',
        'V_M_ONLINE': 'VM Online',
        'SHOPEE_QRIS': 'SHOPEE QRIS',
        'SHOPEE_PAY': 'SHOPEE PAY',
        'PASTAHUT_GOBIZ': 'PASTAHUT GOBIZ',
        'P_POINT_MANDIRI': 'P Point Mandiri',
        'REIMBURSE_SHOPEE': 'REIMBURSE SHOPEE',
        'BNI_POINT': 'BNI Point',
        'BCA_FLAZZ': 'BCA Flazz',
        'CC_DEBIT_ONLINE': 'Cc/Debit Online',
        'LINK_AJA_MANDIRI': 'LINK AJA (Mandiri)',
        'DANA_BALANCE': 'DANA BALANCE',
        'VOUCHER_DIGITAL': 'Voucher Digital',
        'DANA_KREDIT': 'DANA KREDIT',
        'DG_VOUCHER': 'DG Voucher',
        'BCA_QRIS': 'BCA QRIS',
        'BTN': 'BTN',
        'KREDIVO': 'Kredivo',
        'VOUCHER_SODEXO_PLUXEE': 'Voucher Sodexo/Pluxee',
        'INDODANA': 'Indodana',
        'Harian / Bulanan': 'Harian / Bulanan',
        'Nama_File': 'Nama File',
        }

        base_df = base_df.rename(columns = mapping)
        
        desired_order = [
            'store_code', 'business_date', 'BILL EAT IN', 'BILL DINE IN',
            'BILL TAKE AWAY', 'BILL DELIVERY', 'BILL AGGREGATOR',
            'FOOD GROSS SALES EAT IN', 'FOOD GROSS SALES DINE IN',
            'FOOD GROSS SALES TAKE AWAY', 'FOOD GROSS SALES DELIVERY',
            'FOOD GROSS SALES AGGREGATOR', 'BVRG GROSS SALES EAT IN',
            'BVRG GROSS SALES DINE IN', 'BVRG GROSS SALES TAKE AWAY',
            'BVRG GROSS SALES DELIVERY', 'BVRG GROSS SALES AGGREGATOR',
            'DISCOUNT FOOD EAT IN', 'DISCOUNT FOOD DINE IN',
            'DISCOUNT FOOD TAKE AWAY', 'DISCOUNT FOOD DELIVERY',
            'DISCOUNT FOOD AGGREGATOR', 'DISCOUNT BVRG EAT IN',
            'DISCOUNT BVRG DINE IN', 'DISCOUNT BVRG TAKE AWAY',
            'DISCOUNT BVRG DELIVERY', 'DISCOUNT BVRG AGGREGATOR',
            'DELIVERY COST TOTAL', 'TAKEAWAY COST EAT IN', 'TAKEAWAY COST DINE IN', 'TAKEAWAY COST TAKE AWAY', 'TAKEAWAY COST AGGREGATOR', 
            'RESTAURANT TAX EAT IN', 'RESTAURANT TAX DINE IN', 'RESTAURANT TAX TAKE AWAY',
            'RESTAURANT TAX DELIVERY', 'RESTAURANT TAX AGGREGATOR',
            'Cash', 'IM GRAB', 'GO-BIZ', 'BCA VA', 'BCA EDC DBT', 'OVO ONLINE',
            'BNI EDC VM', 'REIMBURSE IM GRAB', 'Other Payment', 'BCA EDCV VM',
            'REIMBURSE GOBIZ', 'MANDIRI VMD', 'CIMB QRIS', 'GO-PAY ONLINE',
            'BCA EDC BCA', 'BCA RWD', 'BRI V/M', 'AMEX', 'OVO', 'V/M ONLINE',
            'MANDIRI QR LIVIN', 'GOPAY ONLINE', 'SHOPEEPAY ONLINE', 'SHOPEEFOOD',
            'GO-PAY', 'Livin MANDIRI', 'SHOPEE ONLINE', 'PASTAHUT IMGRAB',
            'Voucher PH', 'VM Online', 'SHOPEE QRIS', 'SHOPEE PAY',
            'PASTAHUT GOBIZ', 'P Point Mandiri', 'REIMBURSE SHOPEE', 'BNI Point',
            'BCA Flazz', 'Cc/Debit Online', 'LINK AJA (Mandiri)', 'DANA BALANCE',
            'Voucher Digital', 'DANA KREDIT', 'DG Voucher', 'BCA QRIS', 'BTN',
            'Kredivo', 'Voucher Sodexo/Pluxee', 'Indodana', 'Harian / Bulanan', 'Nama File',
        ]
        result_df = base_df.reindex(columns = desired_order)
        return result_df

    def save_to_csv(self, result_df2, start_date, end_date):
        file_name = f"monthly_SMK_HARIAN {start_date.replace('-', '_')}_to_{end_date.replace('-', '_')}.csv"
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
          business_date,
          --total_bill--
          0 as Bill_Eat_In,
          COUNT(CASE WHEN segment = "Dine-in" THEN order_id END) AS Bill_Dine_In,
          COUNT(CASE WHEN segment = "Takeaway" THEN order_id END) AS Bill_Takeaway,
          COUNT(CASE WHEN segment = "Delivery" THEN order_id END) as Bill_Delivery,
          COUNT(CASE WHEN segment = "Aggregator" THEN order_id END) as Bill_Aggregator,

          --total discount--
          0 as Discount_Food_Eat_In,
          SUM(CASE WHEN segment = "Dine-in" THEN discount ELSE 0 END) as Discount_Food_Dine_In,
          SUM(CASE WHEN segment = "Takeaway" THEN discount ELSE 0 END) as Discount_Food_Takeaway,
          SUM(CASE WHEN segment = "Delivery" THEN discount ELSE 0 END) as Discount_Food_Delivery,
          SUM(CASE WHEN segment = "Aggregator" THEN discount ELSE 0 END) as Discount_Food_Aggregator,
          0 as Discount_BVRG_Eat_In,
          0 as Discount_BVRG_Dine_In,
          0 as Discount_BVRG_Takeaway,
          0 as Discount_BVRG_Delivery,
          0 as Discount_BVRG_Aggregator,

          --total delfee--
          SUM(CASE WHEN segment = "Delivery" THEN delivery_fee ELSE 0 END) as Delivery_Cost_Total,
          
          --total takeaway charge--
          0 as Takaway_Cost_Eat_In,
          SUM(CASE WHEN segment = "Dine-in" THEN delivery_fee ELSE 0 END) as Takeaway_Cost_Dine_In,
          SUM(CASE WHEN segment = "Takeaway" THEN delivery_fee ELSE 0 END) as Takeaway_Cost_Take_Away,
          SUM(CASE WHEN segment = "Aggregator" THEN delivery_fee ELSE 0 END) as Takeaway_Cost_Aggregator,

          --total tax--
          0 as Restaurant_Tax_Eat_In,
          SUM(CASE WHEN segment = 'Dine-in' THEN tax ELSE 0 END) as Restaurant_Tax_Dine_In,
          SUM(CASE WHEN segment = 'Takeaway' THEN tax ELSE 0 END) as Restaurant_Tax_Takeaway,
          SUM(CASE WHEN segment = 'Delivery' THEN tax ELSE 0 END) as Restaurant_Tax_Delivery,
          SUM(CASE WHEN segment = 'Aggregator' THEN tax ELSE 0 END) as Restaurant_Tax_Aggregator,
        FROM
          `phi-gcp-2021.analytics.orders_staging`
        WHERE
          business_date between "{start_date}" and "{end_date}"
        GROUP BY
          store_code, business_date
        ORDER BY
          store_code, business_date asc;
        """
        df1 = self.execute_query(sql_code_1)

        # SQL Code 2
        sql_code_2 = f"""
        SELECT
          od.store_code,
          od.business_date,
          --food gross sales--
          0 as Food_Gross_Sales_Eat_In,
          SUM(CASE WHEN od.product_category_name NOT IN ("Beverage", "Pasta Hut Drink", "Coffee Premium") and o.segment = "Dine-in" THEN od.gross_sales ELSE 0 END) AS Food_Gross_Sales_Dine_In,
          SUM(CASE WHEN od.product_category_name NOT IN ("Beverage", "Pasta Hut Drink", "Coffee Premium") and o.segment = "Takeaway" THEN od.gross_sales ELSE 0 END) AS Food_Gross_Sales_Takeaway,
          SUM(CASE WHEN od.product_category_name NOT IN ("Beverage", "Pasta Hut Drink", "Coffee Premium") and o.segment = "Delivery" THEN od.gross_sales ELSE 0 END) AS Food_Gross_Sales_Delivery,
          SUM(CASE WHEN od.product_category_name NOT IN ("Beverage", "Pasta Hut Drink", "Coffee Premium") and o.segment = "Aggregator" THEN od.gross_sales ELSE 0 END) AS Food_Gross_Sales_Aggregator,

          --bvrg gross sales--
          0 as BVRG_Gross_Sales_Eat_In,
          SUM(CASE WHEN od.product_category_name IN ("Beverage", "Pasta Hut Drink", "Coffee Premium") and o.segment = "Dine-in" THEN od.gross_sales ELSE 0 END) AS BVRG_Gross_Sales_Dine_In,
          SUM(CASE WHEN od.product_category_name IN ("Beverage", "Pasta Hut Drink", "Coffee Premium") and o.segment = "Takeaway" THEN od.gross_sales ELSE 0 END) AS BVRG_Gross_Sales_Takeaway,
          SUM(CASE WHEN od.product_category_name IN ("Beverage", "Pasta Hut Drink", "Coffee Premium") and o.segment = "Delivery" THEN od.gross_sales ELSE 0 END) AS BVRG_Gross_Sales_Delivery,
          SUM(CASE WHEN od.product_category_name IN ("Beverage", "Pasta Hut Drink", "Coffee Premium") and o.segment = "Aggregator" THEN od.gross_sales ELSE 0 END) AS BVRG_Gross_Sales_Aggregator,
        FROM
          `phi-gcp-2021.analytics.order_details_staging` as od
        LEFT JOIN
          `phi-gcp-2021.analytics.orders_staging` as o
        ON
          od.order_id = o.order_id
        WHERE
          od.business_date between "{start_date}" and "{end_date}"
        GROUP BY
          od.store_code, od.business_date
        ORDER BY
          od.store_code, od.business_date asc;
        """
        df2 = self.execute_query(sql_code_2)

        # SQL Code 3
        sql_code_3 = f"""
        SELECT
          store_code,
          business_date,
          --payment_code--
          SUM(CASE WHEN payment_code = 1 THEN payment_value ELSE 0 END) AS Cash,
          SUM(CASE WHEN payment_code = 3 THEN payment_value ELSE 0 END) AS BCA_EDCV_VM,
          SUM(CASE WHEN payment_code = 4 THEN payment_value ELSE 0 END) AS BNI_EDC_VM,
          SUM(CASE WHEN payment_code = 5 THEN payment_value ELSE 0 END) AS BCA_EDC_BCA,

          SUM(CASE WHEN payment_code = 6 THEN payment_value ELSE 0 END) as BCA_FLAZZ,
          SUM(CASE WHEN payment_code = 7 THEN payment_value ELSE 0 END) as OVO,
          SUM(CASE WHEN payment_code = 8 THEN payment_value ELSE 0 END) as MANDIRI_VMD,
          SUM(CASE WHEN payment_code = 9 THEN payment_value ELSE 0 END) as VOUCHER_PH,

          SUM(CASE WHEN payment_code = 10 THEN payment_value ELSE 0 END) as AMEX,
          SUM(CASE WHEN payment_code = 13 THEN payment_value ELSE 0 END) as BCA_EDC_DBT,
          SUM(CASE WHEN payment_code = 14 THEN payment_value ELSE 0 END) as BCA_RWD,
          SUM(CASE WHEN payment_code = 16 THEN payment_value ELSE 0 END) as VOUCHER_DIGITAL,

          SUM(CASE WHEN payment_code = 18 THEN payment_value ELSE 0 END) as OTHER_PAYMENT,
          SUM(CASE WHEN payment_code = 22 THEN payment_value ELSE 0 END) as CC_DEBIT_ONLINE,
          SUM(CASE WHEN payment_code = 26 THEN payment_value ELSE 0 END) as VM_ONLINE,
          SUM(CASE WHEN payment_code = 27 THEN payment_value ELSE 0 END) as BCA_VA,

          SUM(CASE WHEN payment_code = 32 THEN payment_value ELSE 0 END) as BNI_POINT,
          SUM(CASE WHEN payment_code = 33 THEN payment_value ELSE 0 END) as LIVIN_MANDIRI,
          SUM(CASE WHEN payment_code = 34 THEN payment_value ELSE 0 END) as DANA_BALANCE,
          SUM(CASE WHEN payment_code = 35 THEN payment_value ELSE 0 END) as DANA_KREDIT,

          SUM(CASE WHEN payment_code = 37 THEN payment_value ELSE 0 END) as LINK_AJA_MANDIRI,
          SUM(CASE WHEN payment_code = 40 THEN payment_value ELSE 0 END) as GOPAY_ONLINE,
          SUM(CASE WHEN payment_code = 43 THEN payment_value ELSE 0 END) as BRI_VM,
          SUM(CASE WHEN payment_code = 47 THEN payment_value ELSE 0 END) as DG_VOUCHER,

          SUM(CASE WHEN payment_code = 50 THEN payment_value ELSE 0 END) as GOPAY,
          SUM(CASE WHEN payment_code = 51 THEN payment_value ELSE 0 END) as GO_BIZ,
          SUM(CASE WHEN payment_code = 52 THEN payment_value ELSE 0 END) as IM_GRAB,
          SUM(CASE WHEN payment_code = 53 THEN payment_value ELSE 0 END) as SHOPEE_PAY,

          SUM(CASE WHEN payment_code = 54 THEN payment_value ELSE 0 END) as SHOPEEPAY_ONLINE,
          SUM(CASE WHEN payment_code = 55 THEN payment_value ELSE 0 END) as SHOPEE_QRIS,
          SUM(CASE WHEN payment_code = 76 THEN payment_value ELSE 0 END) as REIMBURSE_GOBIZ,
          SUM(CASE WHEN payment_code = 77 THEN payment_value ELSE 0 END) as REIMBURSE_IM_GRAB,

          SUM(CASE WHEN payment_code = 78 THEN payment_value ELSE 0 END) as REIMBURSE_SHOPEE,
          SUM(CASE WHEN payment_code = 79 THEN payment_value ELSE 0 END) as OVO_ONLINE,
          SUM(CASE WHEN payment_code = 80 THEN payment_value ELSE 0 END) as MANDIRI_QR_LIVIN,
          SUM(CASE WHEN payment_code = 81 THEN payment_value ELSE 0 END) as PASTAHUT_IMGRAB,

          SUM(CASE WHEN payment_code = 89 THEN payment_value ELSE 0 END) as PASTAHUT_GOBIZ,
          SUM(CASE WHEN payment_code = 100 THEN payment_value ELSE 0 END) as SHOPEEFOOD, 
          SUM(CASE WHEN payment_code = 101 THEN payment_value ELSE 0 END) as CIMB_QRIS,
          SUM(CASE WHEN payment_code = 102 THEN payment_value ELSE 0 END) as BCA_QRIS,
          0 as GO_PAY_ONLINE,
          0 as SHOPEE_ONLINE,
          0 as V_M_ONLINE,
          0 as P_POINT_MANDIRI,

          SUM(CASE WHEN payment_code = 103 THEN payment_value ELSE 0 END) as KREDIVO,
          SUM(CASE WHEN payment_code = 104 THEN payment_value ELSE 0 END) as BTN,
          SUM(CASE WHEN payment_code = 105 THEN payment_value ELSE 0 END) as VOUCHER_SODEXO_PLUXEE,
          SUM(CASE WHEN payment_code = 106 THEN payment_value ELSE 0 END) as INDODANA
        FROM 
          `phi-gcp-2021.analytics.order_payment_staging`
        WHERE
          business_date between "{start_date}" and "{end_date}"
        GROUP BY 
          store_code, business_date
        ORDER BY
          store_code, business_date asc;
        """
        df3 = self.execute_query(sql_code_3)
        
        print("Merging... :)")
        print("="*70)
        
        for _ in tqdm(range(len(df1)), desc="Merging Progress", unit="row"):
            time.sleep(0.01)

        result_df = pd.merge(pd.merge(df1,df2, on=['store_code', 'business_date']),df3,on= ['store_code', 'business_date'])
        
        for _ in tqdm(range(20), desc="Merging Progress", unit="second"):
            time.sleep(0.01)
        print("="*70)
        print("Finished !!!")
        
        result_df_copy = result_df.copy()
        result_df2 = self.append_rows(result_df_copy)
        self.save_to_csv(result_df2, start_date, end_date)

        return result_df, result_df2    
    
# class get data sales per outlet
class QueryExecutor_harian_peroutlet:
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
                print("REQUIRED LIBRARY")
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

    def run_queries_with_libraries_check(self):
        self.check_and_install_libraries()
        while True:
            print(" ")
            print(Fore.GREEN + "Program Started")
            print("=" * 70)
            print(Fore.GREEN + "Program Name : Get Data Sales Big Query Summary Harian")
            print("=" * 70)
            
            self.get_password()
            
            if self.password == self.default_password:
                print("Authentication successful!\n")
            else:
                print("Authentication failed. Please check your password.")
                break
            
            outlet = input("Enter Outlet Code (Contoh : B001): ")
            start_date = input("Enter Start Date Format YYYY-MM-DD: ")
            end_date = input("Enter End Date Format YYYY-MM-DD: ")
            
            result_df, result_df2 = self.run_queries(start_date, end_date, outlet)
            
            result_df_selected = result_df.iloc[:, 0:9]
            result_df2_selected = result_df2.iloc[:, 0:9]
            
            headers_df = result_df_selected.columns.tolist()
            headers_df2 = result_df2_selected.columns.tolist()
            
            print("\nResult Original DataFrame Preview:")
            print(tabulate(result_df_selected.head(32), headers_df, tablefmt='psql'))
            print("\nResult After Process DataFrame 2 Preview:")
            print(tabulate(result_df2_selected.head(32), headers_df2, tablefmt='psql'))
            print("\nWorking finished !!!")
            run_again = input("Do you want to run the program again? (Y/N): ").upper()
            return run_again

    def execute_query(self, sql_code):
        return pandas_gbq.read_gbq(sql_code, project_id=self.project_id)
    
    def append_rows(self, base_df):
        base_df['New_Column'] = base_df['business_date'].isnull().astype(int)
        base_df = base_df.sort_values(by=['store_code', 'New_Column', 'business_date'], ascending=[True, True, True])
        base_df['Harian / Bulanan'] = np.where(base_df['New_Column'] == 0, '1 Hari', '1 Bulan')
        base_df['Nama_File'] = "Vrooh933.csv"
        base_df = base_df.drop(columns=['New_Column'])
        
        # crete mapping
        mapping = {
        'Bill_Eat_In': 'BILL EAT IN',
        'Bill_Dine_In': 'BILL DINE IN',
        'Bill_Takeaway': 'BILL TAKE AWAY',
        'Bill_Delivery': 'BILL DELIVERY',
        'Bill_Aggregator': 'BILL AGGREGATOR',
        'Food_Gross_Sales_Eat_In': 'FOOD GROSS SALES EAT IN',
        'Food_Gross_Sales_Dine_In': 'FOOD GROSS SALES DINE IN',
        'Food_Gross_Sales_Takeaway': 'FOOD GROSS SALES TAKE AWAY',
        'Food_Gross_Sales_Delivery': 'FOOD GROSS SALES DELIVERY',
        'Food_Gross_Sales_Aggregator': 'FOOD GROSS SALES AGGREGATOR',
        'BVRG_Gross_Sales_Eat_In': 'BVRG GROSS SALES EAT IN',
        'BVRG_Gross_Sales_Dine_In': 'BVRG GROSS SALES DINE IN',
        'BVRG_Gross_Sales_Takeaway': 'BVRG GROSS SALES TAKE AWAY',
        'BVRG_Gross_Sales_Delivery': 'BVRG GROSS SALES DELIVERY',
        'BVRG_Gross_Sales_Aggregator': 'BVRG GROSS SALES AGGREGATOR',
        'Discount_Food_Eat_In': 'DISCOUNT FOOD EAT IN',
        'Discount_Food_Dine_In': 'DISCOUNT FOOD DINE IN',
        'Discount_Food_Takeaway': 'DISCOUNT FOOD TAKE AWAY',
        'Discount_Food_Delivery': 'DISCOUNT FOOD DELIVERY',
        'Discount_Food_Aggregator': 'DISCOUNT FOOD AGGREGATOR',
        'Discount_BVRG_Eat_In': 'DISCOUNT BVRG EAT IN',
        'Discount_BVRG_Dine_In': 'DISCOUNT BVRG DINE IN',
        'Discount_BVRG_Takeaway': 'DISCOUNT BVRG TAKE AWAY',
        'Discount_BVRG_Delivery': 'DISCOUNT BVRG DELIVERY',
        'Discount_BVRG_Aggregator': 'DISCOUNT BVRG AGGREGATOR',
        'Delivery_Cost_Total': 'DELIVERY COST TOTAL',
        'Takaway_Cost_Eat_In': 'TAKEAWAY COST EAT IN',
        'Takeaway_Cost_Dine_In': 'TAKEAWAY COST DINE IN',
        'Takeaway_Cost_Take_Away': 'TAKEAWAY COST TAKE AWAY',
        'Takeaway_Cost_Aggregator': 'TAKEAWAY COST AGGREGATOR',
        'Restaurant_Tax_Eat_In': 'RESTAURANT TAX EAT IN',
        'Restaurant_Tax_Dine_In': 'RESTAURANT TAX DINE IN',
        'Restaurant_Tax_Takeaway': 'RESTAURANT TAX TAKE AWAY',
        'Restaurant_Tax_Delivery': 'RESTAURANT TAX DELIVERY',
        'Restaurant_Tax_Aggregator': 'RESTAURANT TAX AGGREGATOR',
        'Cash': 'Cash',
        'IM_GRAB': 'IM GRAB',
        'GO_BIZ': 'GO-BIZ',
        'BCA_VA': 'BCA VA',
        'BCA_EDC_DBT': 'BCA EDC DBT',
        'OVO_ONLINE': 'OVO ONLINE',
        'BNI_EDC_VM': 'BNI EDC VM',
        'REIMBURSE_IM_GRAB': 'REIMBURSE IM GRAB',
        'OTHER_PAYMENT': 'Other Payment',
        'BCA_EDCV_VM': 'BCA EDCV VM',
        'REIMBURSE_GOBIZ': 'REIMBURSE GOBIZ',
        'MANDIRI_VMD': 'MANDIRI VMD',
        'CIMB_QRIS': 'CIMB QRIS',
        'GO_PAY_ONLINE': 'GO-PAY ONLINE',
        'BCA_EDC_BCA': 'BCA EDC BCA',
        'BCA_RWD': 'BCA RWD',
        'BRI_VM': 'BRI V/M',
        'AMEX': 'AMEX',
        'OVO': 'OVO',
        'VM_ONLINE': 'V/M ONLINE',
        'MANDIRI_QR_LIVIN': 'MANDIRI QR LIVIN',
        'GOPAY_ONLINE': 'GOPAY ONLINE',
        'SHOPEEPAY_ONLINE': 'SHOPEEPAY ONLINE',
        'SHOPEEFOOD': 'SHOPEEFOOD',
        'GOPAY': 'GO-PAY',
        'LIVIN_MANDIRI': 'Livin MANDIRI',
        'SHOPEE_ONLINE': 'SHOPEE ONLINE',
        'PASTAHUT_IMGRAB': 'PASTAHUT IMGRAB',
        'VOUCHER_PH': 'Voucher PH',
        'V_M_ONLINE': 'VM Online',
        'SHOPEE_QRIS': 'SHOPEE QRIS',
        'SHOPEE_PAY': 'SHOPEE PAY',
        'PASTAHUT_GOBIZ': 'PASTAHUT GOBIZ',
        'P_POINT_MANDIRI': 'P Point Mandiri',
        'REIMBURSE_SHOPEE': 'REIMBURSE SHOPEE',
        'BNI_POINT': 'BNI Point',
        'BCA_FLAZZ': 'BCA Flazz',
        'CC_DEBIT_ONLINE': 'Cc/Debit Online',
        'LINK_AJA_MANDIRI': 'LINK AJA (Mandiri)',
        'DANA_BALANCE': 'DANA BALANCE',
        'VOUCHER_DIGITAL': 'Voucher Digital',
        'DANA_KREDIT': 'DANA KREDIT',
        'DG_VOUCHER': 'DG Voucher',
        'BCA_QRIS': 'BCA QRIS',
        'BTN': 'BTN',
        'KREDIVO': 'Kredivo',
        'VOUCHER_SODEXO_PLUXEE': 'Voucher Sodexo/Pluxee',
        'INDODANA':'Indodana',
        'Harian / Bulanan': 'Harian / Bulanan',
        'Nama_File': 'Nama File',
        }

        base_df = base_df.rename(columns = mapping)
        
        desired_order = [
            'store_code', 'business_date', 'BILL EAT IN', 'BILL DINE IN',
            'BILL TAKE AWAY', 'BILL DELIVERY', 'BILL AGGREGATOR',
            'FOOD GROSS SALES EAT IN', 'FOOD GROSS SALES DINE IN',
            'FOOD GROSS SALES TAKE AWAY', 'FOOD GROSS SALES DELIVERY',
            'FOOD GROSS SALES AGGREGATOR', 'BVRG GROSS SALES EAT IN',
            'BVRG GROSS SALES DINE IN', 'BVRG GROSS SALES TAKE AWAY',
            'BVRG GROSS SALES DELIVERY', 'BVRG GROSS SALES AGGREGATOR',
            'DISCOUNT FOOD EAT IN', 'DISCOUNT FOOD DINE IN',
            'DISCOUNT FOOD TAKE AWAY', 'DISCOUNT FOOD DELIVERY',
            'DISCOUNT FOOD AGGREGATOR', 'DISCOUNT BVRG EAT IN',
            'DISCOUNT BVRG DINE IN', 'DISCOUNT BVRG TAKE AWAY',
            'DISCOUNT BVRG DELIVERY', 'DISCOUNT BVRG AGGREGATOR',
            'DELIVERY COST TOTAL', 'TAKEAWAY COST EAT IN', 'TAKEAWAY COST DINE IN', 'TAKEAWAY COST TAKE AWAY', 'TAKEAWAY COST AGGREGATOR', 
            'RESTAURANT TAX EAT IN', 'RESTAURANT TAX DINE IN', 'RESTAURANT TAX TAKE AWAY',
            'RESTAURANT TAX DELIVERY', 'RESTAURANT TAX AGGREGATOR',
            'Cash', 'IM GRAB', 'GO-BIZ', 'BCA VA', 'BCA EDC DBT', 'OVO ONLINE',
            'BNI EDC VM', 'REIMBURSE IM GRAB', 'Other Payment', 'BCA EDCV VM',
            'REIMBURSE GOBIZ', 'MANDIRI VMD', 'CIMB QRIS', 'GO-PAY ONLINE',
            'BCA EDC BCA', 'BCA RWD', 'BRI V/M', 'AMEX', 'OVO', 'V/M ONLINE',
            'MANDIRI QR LIVIN', 'GOPAY ONLINE', 'SHOPEEPAY ONLINE', 'SHOPEEFOOD',
            'GO-PAY', 'Livin MANDIRI', 'SHOPEE ONLINE', 'PASTAHUT IMGRAB',
            'Voucher PH', 'VM Online', 'SHOPEE QRIS', 'SHOPEE PAY',
            'PASTAHUT GOBIZ', 'P Point Mandiri', 'REIMBURSE SHOPEE', 'BNI Point',
            'BCA Flazz', 'Cc/Debit Online', 'LINK AJA (Mandiri)', 'DANA BALANCE',
            'Voucher Digital', 'DANA KREDIT', 'DG Voucher', 'BCA QRIS', 'BTN',
            'Kredivo', 'Voucher Sodexo/Pluxee', 'Indodana','Harian / Bulanan', 'Nama File',
        ]
        result_df = base_df.reindex(columns = desired_order)
        return result_df

    def save_to_csv(self, result_df2, start_date, end_date, outlet):
        file_name = f"monthly_SMK_HARIAN_{outlet}_{start_date.replace('-', '_')}_to_{end_date.replace('-', '_')}.csv"
        download_folder = os.path.expanduser("~/Downloads")
        file_path = os.path.join(download_folder, file_name)
        result_df2.to_csv(file_path, index=False)
        print(f"Dataframe berhasil disimpan di: {file_path}")
    
    def run_queries(self, start_date, end_date, outlet):
        # SQL Code 1
        sql_code_1 = f"""
        --raw data Bill
        SELECT
          store_code,
          business_date,
          --total_bill--
          0 as Bill_Eat_In,
          COUNT(CASE WHEN segment = "Dine-in" THEN order_id END) AS Bill_Dine_In,
          COUNT(CASE WHEN segment = "Takeaway" THEN order_id END) AS Bill_Takeaway,
          COUNT(CASE WHEN segment = "Delivery" THEN order_id END) as Bill_Delivery,
          COUNT(CASE WHEN segment = "Aggregator" THEN order_id END) as Bill_Aggregator,

          --total discount--
          0 as Discount_Food_Eat_In,
          SUM(CASE WHEN segment = "Dine-in" THEN discount ELSE 0 END) as Discount_Food_Dine_In,
          SUM(CASE WHEN segment = "Takeaway" THEN discount ELSE 0 END) as Discount_Food_Takeaway,
          SUM(CASE WHEN segment = "Delivery" THEN discount ELSE 0 END) as Discount_Food_Delivery,
          SUM(CASE WHEN segment = "Aggregator" THEN discount ELSE 0 END) as Discount_Food_Aggregator,
          0 as Discount_BVRG_Eat_In,
          0 as Discount_BVRG_Dine_In,
          0 as Discount_BVRG_Takeaway,
          0 as Discount_BVRG_Delivery,
          0 as Discount_BVRG_Aggregator,

          --total delfee--
          SUM(CASE WHEN segment = "Delivery" THEN delivery_fee ELSE 0 END) as Delivery_Cost_Total,
          
          --total takeaway charge--
          0 as Takaway_Cost_Eat_In,
          SUM(CASE WHEN segment = "Dine-in" THEN delivery_fee ELSE 0 END) as Takeaway_Cost_Dine_In,
          SUM(CASE WHEN segment = "Takeaway" THEN delivery_fee ELSE 0 END) as Takeaway_Cost_Take_Away,
          SUM(CASE WHEN segment = "Aggregator" THEN delivery_fee ELSE 0 END) as Takeaway_Cost_Aggregator,

          --total tax--
          0 as Restaurant_Tax_Eat_In,
          SUM(CASE WHEN segment = 'Dine-in' THEN tax ELSE 0 END) as Restaurant_Tax_Dine_In,
          SUM(CASE WHEN segment = 'Takeaway' THEN tax ELSE 0 END) as Restaurant_Tax_Takeaway,
          SUM(CASE WHEN segment = 'Delivery' THEN tax ELSE 0 END) as Restaurant_Tax_Delivery,
          SUM(CASE WHEN segment = 'Aggregator' THEN tax ELSE 0 END) as Restaurant_Tax_Aggregator,
        FROM
          `phi-gcp-2021.analytics.orders_staging`
        WHERE
          business_date between "{start_date}" and "{end_date}" and store_code = "{outlet}"
        GROUP BY
          store_code, business_date
        ORDER BY
          store_code, business_date asc;
        """
        df1 = self.execute_query(sql_code_1)

        # SQL Code 2
        sql_code_2 = f"""
        SELECT
          od.store_code,
          od.business_date,
          --food gross sales--
          0 as Food_Gross_Sales_Eat_In,
          SUM(CASE WHEN od.product_category_name NOT IN ("Beverage", "Pasta Hut Drink", "Coffee Premium") and o.segment = "Dine-in" THEN od.gross_sales ELSE 0 END) AS Food_Gross_Sales_Dine_In,
          SUM(CASE WHEN od.product_category_name NOT IN ("Beverage", "Pasta Hut Drink", "Coffee Premium") and o.segment = "Takeaway" THEN od.gross_sales ELSE 0 END) AS Food_Gross_Sales_Takeaway,
          SUM(CASE WHEN od.product_category_name NOT IN ("Beverage", "Pasta Hut Drink", "Coffee Premium") and o.segment = "Delivery" THEN od.gross_sales ELSE 0 END) AS Food_Gross_Sales_Delivery,
          SUM(CASE WHEN od.product_category_name NOT IN ("Beverage", "Pasta Hut Drink", "Coffee Premium") and o.segment = "Aggregator" THEN od.gross_sales ELSE 0 END) AS Food_Gross_Sales_Aggregator,

          --bvrg gross sales--
          0 as BVRG_Gross_Sales_Eat_In,
          SUM(CASE WHEN od.product_category_name IN ("Beverage", "Pasta Hut Drink", "Coffee Premium") and o.segment = "Dine-in" THEN od.gross_sales ELSE 0 END) AS BVRG_Gross_Sales_Dine_In,
          SUM(CASE WHEN od.product_category_name IN ("Beverage", "Pasta Hut Drink", "Coffee Premium") and o.segment = "Takeaway" THEN od.gross_sales ELSE 0 END) AS BVRG_Gross_Sales_Takeaway,
          SUM(CASE WHEN od.product_category_name IN ("Beverage", "Pasta Hut Drink", "Coffee Premium") and o.segment = "Delivery" THEN od.gross_sales ELSE 0 END) AS BVRG_Gross_Sales_Delivery,
          SUM(CASE WHEN od.product_category_name IN ("Beverage", "Pasta Hut Drink", "Coffee Premium") and o.segment = "Aggregator" THEN od.gross_sales ELSE 0 END) AS BVRG_Gross_Sales_Aggregator,
        FROM
          `phi-gcp-2021.analytics.order_details_staging` as od
        LEFT JOIN
          `phi-gcp-2021.analytics.orders_staging` as o
        ON
          od.order_id = o.order_id
        WHERE
          od.business_date between "{start_date}" and "{end_date}" and od.store_code = "{outlet}"
        GROUP BY
          od.store_code, od.business_date
        ORDER BY
          od.store_code, od.business_date asc;
        """
        df2 = self.execute_query(sql_code_2)

        # SQL Code 3
        sql_code_3 = f"""
        SELECT
          store_code,
          business_date,
          --payment_code--
          SUM(CASE WHEN payment_code = 1 THEN payment_value ELSE 0 END) AS Cash,
          SUM(CASE WHEN payment_code = 3 THEN payment_value ELSE 0 END) AS BCA_EDCV_VM,
          SUM(CASE WHEN payment_code = 4 THEN payment_value ELSE 0 END) AS BNI_EDC_VM,
          SUM(CASE WHEN payment_code = 5 THEN payment_value ELSE 0 END) AS BCA_EDC_BCA,

          SUM(CASE WHEN payment_code = 6 THEN payment_value ELSE 0 END) as BCA_FLAZZ,
          SUM(CASE WHEN payment_code = 7 THEN payment_value ELSE 0 END) as OVO,
          SUM(CASE WHEN payment_code = 8 THEN payment_value ELSE 0 END) as MANDIRI_VMD,
          SUM(CASE WHEN payment_code = 9 THEN payment_value ELSE 0 END) as VOUCHER_PH,

          SUM(CASE WHEN payment_code = 10 THEN payment_value ELSE 0 END) as AMEX,
          SUM(CASE WHEN payment_code = 13 THEN payment_value ELSE 0 END) as BCA_EDC_DBT,
          SUM(CASE WHEN payment_code = 14 THEN payment_value ELSE 0 END) as BCA_RWD,
          SUM(CASE WHEN payment_code = 16 THEN payment_value ELSE 0 END) as VOUCHER_DIGITAL,

          SUM(CASE WHEN payment_code = 18 THEN payment_value ELSE 0 END) as OTHER_PAYMENT,
          SUM(CASE WHEN payment_code = 22 THEN payment_value ELSE 0 END) as CC_DEBIT_ONLINE,
          SUM(CASE WHEN payment_code = 26 THEN payment_value ELSE 0 END) as VM_ONLINE,
          SUM(CASE WHEN payment_code = 27 THEN payment_value ELSE 0 END) as BCA_VA,

          SUM(CASE WHEN payment_code = 32 THEN payment_value ELSE 0 END) as BNI_POINT,
          SUM(CASE WHEN payment_code = 33 THEN payment_value ELSE 0 END) as LIVIN_MANDIRI,
          SUM(CASE WHEN payment_code = 34 THEN payment_value ELSE 0 END) as DANA_BALANCE,
          SUM(CASE WHEN payment_code = 35 THEN payment_value ELSE 0 END) as DANA_KREDIT,

          SUM(CASE WHEN payment_code = 37 THEN payment_value ELSE 0 END) as LINK_AJA_MANDIRI,
          SUM(CASE WHEN payment_code = 40 THEN payment_value ELSE 0 END) as GOPAY_ONLINE,
          SUM(CASE WHEN payment_code = 43 THEN payment_value ELSE 0 END) as BRI_VM,
          SUM(CASE WHEN payment_code = 47 THEN payment_value ELSE 0 END) as DG_VOUCHER,

          SUM(CASE WHEN payment_code = 50 THEN payment_value ELSE 0 END) as GOPAY,
          SUM(CASE WHEN payment_code = 51 THEN payment_value ELSE 0 END) as GO_BIZ,
          SUM(CASE WHEN payment_code = 52 THEN payment_value ELSE 0 END) as IM_GRAB,
          SUM(CASE WHEN payment_code = 53 THEN payment_value ELSE 0 END) as SHOPEE_PAY,

          SUM(CASE WHEN payment_code = 54 THEN payment_value ELSE 0 END) as SHOPEEPAY_ONLINE,
          SUM(CASE WHEN payment_code = 55 THEN payment_value ELSE 0 END) as SHOPEE_QRIS,
          SUM(CASE WHEN payment_code = 76 THEN payment_value ELSE 0 END) as REIMBURSE_GOBIZ,
          SUM(CASE WHEN payment_code = 77 THEN payment_value ELSE 0 END) as REIMBURSE_IM_GRAB,

          SUM(CASE WHEN payment_code = 78 THEN payment_value ELSE 0 END) as REIMBURSE_SHOPEE,
          SUM(CASE WHEN payment_code = 79 THEN payment_value ELSE 0 END) as OVO_ONLINE,
          SUM(CASE WHEN payment_code = 80 THEN payment_value ELSE 0 END) as MANDIRI_QR_LIVIN,
          SUM(CASE WHEN payment_code = 81 THEN payment_value ELSE 0 END) as PASTAHUT_IMGRAB,

          SUM(CASE WHEN payment_code = 89 THEN payment_value ELSE 0 END) as PASTAHUT_GOBIZ,
          SUM(CASE WHEN payment_code = 100 THEN payment_value ELSE 0 END) as SHOPEEFOOD, 
          SUM(CASE WHEN payment_code = 101 THEN payment_value ELSE 0 END) as CIMB_QRIS,
          SUM(CASE WHEN payment_code = 102 THEN payment_value ELSE 0 END) as BCA_QRIS,
          0 as GO_PAY_ONLINE,
          0 as SHOPEE_ONLINE,
          0 as V_M_ONLINE,
          0 as P_POINT_MANDIRI,

          SUM(CASE WHEN payment_code = 103 THEN payment_value ELSE 0 END) as KREDIVO,
          SUM(CASE WHEN payment_code = 104 THEN payment_value ELSE 0 END) as BTN,
          SUM(CASE WHEN payment_code = 105 THEN payment_value ELSE 0 END) as VOUCHER_SODEXO_PLUXEE,
          SUM(CASE WHEN payment_code = 106 THEN payment_value ELSE 0 END) as INDODANA,
        FROM 
          `phi-gcp-2021.analytics.order_payment_staging`
        WHERE
          business_date between "{start_date}" and "{end_date}" and store_code = "{outlet}"
        GROUP BY 
          store_code, business_date
        ORDER BY
          store_code, business_date asc;
        """
        df3 = self.execute_query(sql_code_3)
        
        print("Merging... :)")
        print("="*70)
        
        for _ in tqdm(range(len(df1)), desc="Merging Progress", unit="row"):
            time.sleep(0.01)

        result_df = pd.merge(pd.merge(df1,df2, on=['store_code', 'business_date']),df3,on= ['store_code', 'business_date'])
        
        for _ in tqdm(range(20), desc="Merging Progress", unit="second"):
            time.sleep(0.01)
        print("="*70)
        print("Finished !!!")
        
        result_df_copy = result_df.copy()
        result_df2 = self.append_rows(result_df_copy)
        self.save_to_csv(result_df2, start_date, end_date, outlet)

        return result_df, result_df2

# class discount monthly    
class QueryExecutor_Discount_Monthly:
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
            print(Fore.GREEN + "Program Started")
            print("=" * 70)
            print(Fore.GREEN + "Program Name : Get Data Discount by Date")
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
            print("\nWorking finished !!!")
            run_again = input("Do you want to run the program again? (Y/N): ").upper()
            return run_again

    def execute_query(self, sql_code):
        return pandas_gbq.read_gbq(sql_code, project_id=self.project_id)

    def save_to_csv(self, result_df2, start_date, end_date):
        file_name = f"monthly_DATA_DISCOUNT {start_date.replace('-', '_')}_to_{end_date.replace('-', '_')}.csv"
        download_folder = os.path.expanduser("~/Downloads")
        file_path = os.path.join(download_folder, file_name)
        result_df2.to_csv(file_path, index=False)
        print(f"Dataframe berhasil disimpan di: {file_path}")
    
    def run_queries(self, start_date, end_date):
        # SQL Code 1
        sql_code_1 = f"""
        select
          o.store_code,
          o.store_name,
          o.store_concept,
          o.business_date,
          o.order_number,
          p.card_number,
          p.approval_code,
          c.promotion_name,
          p.payment_code_name,
          o.product_total_cost as gross_sales,
          o.discount,
          p.payment_value
        from
          `phi-gcp-2021.analytics.orders_staging` as o
        inner join
          `phi-gcp-2021.analytics.order_payment_staging` as p
        on
          o.order_id = p.order_id
        inner join
          `phi-gcp-2021.analytics.order_coupon_staging` as c
        on 
          o.order_id = c.order_id
        where
          o.business_date between '{start_date}' and '{end_date}'
        order by
          o.business_date,
          o.store_code,
          o.order_number
          ASC;
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
    
# class discount per outlet
class QueryExecutor_Discount_PerOutlet:
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
            print(Fore.GREEN + "Program Started")
            print("=" * 70)
            print(Fore.GREEN + "Program Name : Get Data Discount Per-Outlet by Date")
            print("=" * 70)
            
            self.get_password()
            
            if self.password == self.default_password:
                print("Authentication successful!\n")
            else:
                print("Authentication failed. Please check your password.")
                break
            
            store_code = input("Enter Store Code (Contoh : B001):")
            start_date = input("Enter Start Date Format YYYY-MM-DD: ")
            end_date = input("Enter End Date Format YYYY-MM-DD: ")
            
            result_df = self.run_queries(start_date, end_date, store_code)
            result_df_selected = result_df.iloc[:, 0:8]
            headers_df = result_df_selected.columns.tolist()
            
            print("\nResult Original DataFrame Preview:")
            print(tabulate(result_df_selected.head(32), headers_df, tablefmt='psql'))
            print("\nWorking finished !!!")
            run_again = input("Do you want to run the program again? (Y/N): ").upper()
            return run_again

    def execute_query(self, sql_code):
        return pandas_gbq.read_gbq(sql_code, project_id=self.project_id)

    def save_to_csv(self, result_df2, start_date, end_date, store_code):
        file_name = f"monthly DATA DISCOUNT Store Code {store_code} {start_date.replace('-', '_')}_to_{end_date.replace('-', '_')}.csv"
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
          o.store_concept,
          o.business_date,
          o.order_number,
          p.card_number,
          p.approval_code,
          c.promotion_name,
          p.payment_code_name,
          o.product_total_cost as gross_sales,
          o.discount,
          p.payment_value
        from
          `phi-gcp-2021.analytics.orders_staging` as o
        inner join
          `phi-gcp-2021.analytics.order_payment_staging` as p
        on
          o.order_id = p.order_id
        inner join
          `phi-gcp-2021.analytics.order_coupon_staging` as c
        on 
          o.order_id = c.order_id
        where
          o.business_date between '{start_date}' and '{end_date}'
          and o.store_code = '{store_code}'
        order by
          o.business_date,
          o.store_code,
          o.order_number
          ASC;
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

# class discount per kode
class QueryExecutor_Discount_PerKode:
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
            print(Fore.GREEN + "Program Name : Get Data Bill Discount Bulanan Per-Kode Discount")
            print("=" * 70)
            
            self.get_password()
            
            if self.password == self.default_password:
                print("Authentication successful!\n")
            else:
                print("Authentication failed. Please check your password.")
                break
            
            start_date = input("Enter Start Date Format YYYY-MM-DD: ")
            end_date = input("Enter End Date Format YYYY-MM-DD: ")
            
            # print discount_code
            discount_display = self.run_queries_display(start_date, end_date)
            discount_display_selected = discount_display.iloc[:, 0:2]
            header_discount = discount_display_selected.columns.tolist()
            print(tabulate(discount_display_selected, header_discount, tablefmt='psql'))          

            discount_code = input("Enter Discount Code: ")

            result_df = self.run_queries(start_date, end_date, discount_code)
            result_df_selected = result_df.iloc[:, 0:8]
            headers_df = result_df_selected.columns.tolist()
            
            print("\nResult Original DataFrame Preview:")
            print(tabulate(result_df_selected.head(32), headers_df, tablefmt='psql'))
            print("\nWorking' finished !!!")
            run_again = input("Do you want to run the program again? (Y/N): ").upper()
            return run_again

    def execute_query(self, sql_code):
        return pandas_gbq.read_gbq(sql_code, project_id=self.project_id)

    def save_to_csv(self, result_df2, start_date, end_date, discount_code):
        file_name = f"monthly DATA DISCOUNT Discount Code {discount_code} {start_date.replace('-', '_')}_to_{end_date.replace('-', '_')}.csv"
        download_folder = os.path.expanduser("~/Downloads")
        file_path = os.path.join(download_folder, file_name)
        result_df2.to_csv(file_path, index=False)
        print(f"Dataframe berhasil disimpan di: {file_path}")
    
    def run_queries_display(self, start_date, end_date):
        sql_code_discount = f""" 
        select
          distinct
          promotion_code as discount_code,
          promotion_name as discount_name
        from
          `phi-gcp-2021.analytics.order_coupon_staging`
        where
          business_date between "{start_date}" and "{end_date}"
        """
        df_display = self.execute_query(sql_code_discount)
        discount_display = df_display.copy()
        return discount_display

    def run_queries(self, start_date, end_date, discount_code):
        # SQL Code 1
        sql_code_1 = f"""
        select
          o.store_code,
          o.store_name,
          o.store_concept,
          o.business_date,
          o.order_number,
          p.card_number,
          p.approval_code,
          c.promotion_name,
          p.payment_code_name,
          o.product_total_cost as gross_sales,
          o.discount,
          p.payment_value
        from
          `phi-gcp-2021.analytics.orders_staging` as o
        inner join
          `phi-gcp-2021.analytics.order_payment_staging` as p
        on
          o.order_id = p.order_id
        inner join
          `phi-gcp-2021.analytics.order_coupon_staging` as c
        on 
          o.order_id = c.order_id
        where
          o.business_date between '{start_date}' and '{end_date}'
          and c.promotion_code = '{discount_code}'
        order by
          o.business_date,
          o.store_code,
          o.order_number
          ASC;
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
        self.save_to_csv(result_df, start_date, end_date, discount_code)
        return result_df

# class get data payment per-code
class QueryExecutor_Payment_PerKode:
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
            print(Fore.GREEN + "Program Name : Get Data Payment Per-Kode by Date")
            print("=" * 70)
            
            self.get_password()
            
            if self.password == self.default_password:
                print("Authentication successful!\n")
            else:
                print("Authentication failed. Please check your password.")
                break
            
            start_date = input("Enter Start Date Format YYYY-MM-DD: ")
            end_date = input("Enter End Date Format YYYY-MM-DD: ")
            
            # print discount_code
            payment_display = self.run_queries_display(start_date, end_date)
            payment_display_selected = payment_display.iloc[:, 0:2]
            header_payment = payment_display_selected.columns.tolist()
            print(tabulate(payment_display_selected, header_payment, tablefmt='psql'))          

            discount_code = input("Enter Payment Code: ")

            result_df = self.run_queries(start_date, end_date, discount_code)
            result_df_selected = result_df.iloc[:, 0:8]
            headers_df = result_df_selected.columns.tolist()
            
            print("\nResult Original DataFrame Preview:")
            print(tabulate(result_df_selected.head(32), headers_df, tablefmt='psql'))
            print("\nWorking' finished !!!")
            run_again = input("Do you want to run the program again? (Y/N): ").upper()
            return run_again

    def execute_query(self, sql_code):
        return pandas_gbq.read_gbq(sql_code, project_id=self.project_id)

    def save_to_csv(self, result_df2, start_date, end_date, discount_code):
        file_name = f"monthly DATA PAYMENT Payment Code {discount_code} {start_date.replace('-', '_')}_to_{end_date.replace('-', '_')}.csv"
        download_folder = os.path.expanduser("~/Downloads")
        file_path = os.path.join(download_folder, file_name)
        result_df2.to_csv(file_path, index=False)
        print(f"Dataframe berhasil disimpan di: {file_path}")
    
    def run_queries_display(self, start_date, end_date):
        sql_code_discount = f""" 
        select
          distinct
          payment_code as payment_code,
          payment_code_name as payment_name
        from
          `phi-gcp-2021.analytics.order_payment_staging`
        where
          business_date between "{start_date}" and "{end_date}"
        order by
          payment_code asc;
        """
        df_display = self.execute_query(sql_code_discount)
        payment_display = df_display.copy()
        return payment_display

    def run_queries(self, start_date, end_date, payment_code):
        # SQL Code 1
        sql_code_1 = f"""
        select
          o.store_code,
          o.store_name,
          o.business_date,
          o.order_number,
          o.phone_62,
          
          py.card_number,
          py.card_name,
          py.approval_code,
          py.payment_code_name,
          py.remark,

          o.product_total_cost as gross_sales,
          py.payment_value
        from
          `phi-gcp-2021.analytics.orders_staging` as o
        inner join
          `phi-gcp-2021.analytics.order_payment_staging` as py
        on
          o.order_id = py.order_id
        where
          o.business_date between "{start_date}" and "{end_date}"
          and py.payment_code = {payment_code}
        order by 
          o.store_code,
          o.business_date,
          o.order_number;
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
        self.save_to_csv(result_df, start_date, end_date, payment_code)
        return result_df

# Class payment per outlet
class QueryExecutor_Payment_PerOutlet:
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
            return run_again
            
    def execute_query(self, sql_code):
        return pandas_gbq.read_gbq(sql_code, project_id=self.project_id)

    def save_to_csv(self, result_df2, start_date, end_date, store_code):
        file_name = f"monthly DATA PAYMENT Store Code {store_code} {start_date.replace('-', '_')}_to_{end_date.replace('-', '_')}.csv"
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
          o.phone_62,
          
          py.card_number,
          py.card_name,
          py.approval_code,
          py.payment_code_name,
          py.remark,

          o.product_total_cost as gross_sales,
          py.payment_value
        from
          `phi-gcp-2021.analytics.orders_staging` as o
        inner join
          `phi-gcp-2021.analytics.order_payment_staging` as py
        on
          o.order_id = py.order_id
        where
          o.business_date between "{start_date}" and "{end_date}"
          and o.store_code = "{store_code}"
        order by 
          o.store_code,
          o.business_date,
          o.order_number;
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


# class get data oc
class QueryExecutor_OC_PerKode:
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
            print(Fore.GREEN + "Program Name : Get Data Bill OC Per-Kode by Date")
            print("=" * 70)
            
            self.get_password()
            
            if self.password == self.default_password:
                print("Authentication successful!\n")
            else:
                print("Authentication failed. Please check your password.")
                break
            
            start_date = input("Enter Start Date Format YYYY-MM-DD: ")
            end_date = input("Enter End Date Format YYYY-MM-DD: ")
            
            # print discount_code
            oc_display = self.run_queries_display(start_date, end_date)
            oc_display_selected = oc_display.iloc[:, 0:2]
            header_oc = oc_display_selected.columns.tolist()
            print(tabulate(oc_display_selected, header_oc, tablefmt='psql'))          

            oc_code = input("Enter OC Code: ")

            result_df = self.run_queries(start_date, end_date, oc_code)
            result_df_selected = result_df.iloc[:, 0:8]
            headers_df = result_df_selected.columns.tolist()
            
            print("\nResult Original DataFrame Preview:")
            print(tabulate(result_df_selected.head(32), headers_df, tablefmt='psql'))
            print("\nWorking' finished !!!")
            run_again = input("Do you want to run the program again? (Y/N): ").upper()
            return run_again

    def execute_query(self, sql_code):
        return pandas_gbq.read_gbq(sql_code, project_id=self.project_id)

    def save_to_csv(self, result_df, start_date, end_date, oc_code):
        file_name = f"monthly DATA OC Code {oc_code} {start_date.replace('-', '_')}_to_{end_date.replace('-', '_')}.csv"
        download_folder = os.path.expanduser("~/Downloads")
        file_path = os.path.join(download_folder, file_name)
        result_df.to_csv(file_path, index=False)
        print(f"Dataframe berhasil disimpan di: {file_path}")
    
    def run_queries_display(self, start_date, end_date):
        sql_code_oc = f""" 
        select
          distinct
          payment_code as code_OC,
          payment_code_name as OC_name
        from
          `phi-gcp-2021.analytics.order_payment`
        where
          business_date between "{start_date}" and "{end_date}"
          and is_oc = "Y"
        """
        df_display = self.execute_query(sql_code_oc)
        oc_display = df_display.copy()
        return oc_display

    def run_queries(self, start_date, end_date, oc_code):
        # SQL Code 1
        sql_code_1 = f"""
        select  
          p.store_code,
          p.store_name,
          p.business_date,
          p.order_number,
          p.payment_code_name,
          o.product_total_cost as ammount,
          p.remark
        from
          `phi-gcp-2021.analytics.order_payment` as p
        inner join
          `phi-gcp-2021.analytics.orders` as o
        on
          p.order_code = o.order_code
        where
          p.business_date between "{start_date}" and "{end_date}"
          and p.is_oc = "Y"
          and p.payment_code = {oc_code}
        order by
          p.store_code,
          p.business_date
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
        self.save_to_csv(result_df, start_date, end_date, oc_code)
        return result_df


#Class Get data OC per bulan
class QueryExecutor_OC_PerBulan:
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
            print(Fore.GREEN + "Program Name : Get Data Bill OC Bulanan")
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
            return run_again
            
    def execute_query(self, sql_code):
        return pandas_gbq.read_gbq(sql_code, project_id=self.project_id)

    def save_to_csv(self, result_df, start_date, end_date):
        file_name = f"monthly DATA OC {start_date.replace('-', '_')}_to_{end_date.replace('-', '_')}.csv"
        download_folder = os.path.expanduser("~/Downloads")
        file_path = os.path.join(download_folder, file_name)
        result_df.to_csv(file_path, index=False)
        print(f"Dataframe berhasil disimpan di: {file_path}")
    
    def run_queries(self, start_date, end_date):
        # SQL Code 1
        sql_code_1 = f"""
        select  
          p.store_code,
          p.store_name,
          p.business_date,
          p.order_number,
          p.payment_code_name,
          o.product_total_cost as ammount,
          p.remark
        from
          `phi-gcp-2021.analytics.order_payment` as p
        inner join
          `phi-gcp-2021.analytics.orders` as o
        on
          p.order_code = o.order_code
        where
          p.business_date between "{start_date}" and "{end_date}"
          and p.is_oc = "Y"
        order by
          p.store_code,
          p.business_date
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
    
# Class OC Summary    
class QueryExecutor_OC_Summary:
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
            print(Fore.GREEN + "Program Name : Get Data Bill OC Summary")
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
            return run_again

    def execute_query(self, sql_code):
        return pandas_gbq.read_gbq(sql_code, project_id=self.project_id)

    def save_to_csv(self, result_df, start_date, end_date):
        file_name = f"monthly DATA OC {start_date.replace('-', '_')}_to_{end_date.replace('-', '_')}.csv"
        download_folder = os.path.expanduser("~/Downloads")
        file_path = os.path.join(download_folder, file_name)
        result_df.to_csv(file_path, index=False)
        print(f"Dataframe berhasil disimpan di: {file_path}")
    
    def run_queries(self, start_date, end_date):
        # SQL Code 1
        sql_code_1 = f"""
        select  
          p.store_code,
          p.store_name,
          p.payment_code_name,
          sum(o.product_total_cost) as ammount
        from
          `phi-gcp-2021.analytics.order_payment` as p
        inner join
          `phi-gcp-2021.analytics.orders` as o
        on
          p.order_code = o.order_code
        where
          p.business_date between "{start_date}" and "{end_date}"
          and p.is_oc = "Y"
        group by
          1,2,3
        order by
          p.store_code
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

# Class Summary Lite
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
            print(Fore.GREEN + "Program Name : Get Data Sales Big Query Summary LITE by Date")
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
            return run_again

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


# Class Inventory Cost Control Sales Mix Usage
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
            return run_again

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


# Class PerChannel
class QueryExecutor_PerChannel:
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
            print(Fore.GREEN + "Program Name : Get Data Perchannel Offline/Online by Date")
            print("=" * 70)
            
            self.get_password()
            
            if self.password == self.default_password:
                print("Authentication successful!\n")
            else:
                print("Authentication failed. Please check your password.")
                break
            
            start_date = input("Enter Start Date Format YYYY-MM-DD: ")
            end_date = input("Enter End Date Format YYYY-MM-DD: ")

            hasil_display = self.queries_display(start_date, end_date)
            hasil_display_selected = hasil_display.iloc[:, 0:5]
            float_formatter = "{:,.0f}".format
            columns_to_format = [col for col in hasil_display_selected.columns if hasil_display_selected[col].dtype in ['float64', 'int64']]
            hasil_display_selected_formatted = hasil_display_selected.copy()
            hasil_display_selected_formatted[columns_to_format] = hasil_display_selected_formatted[columns_to_format].map(float_formatter)
            print(hasil_display_selected_formatted)
            
            process = input("Apakah data Sudah benar ? (Y/N): ").upper()
            if process == "Y":
                result_df = self.run_queries(start_date, end_date)
                result_df_selected = result_df.iloc[:, 0:8]
                headers_df = result_df_selected.columns.tolist()
                print("\nResult Original DataFrame Preview:")
                print(tabulate(result_df_selected.head(32), headers_df, tablefmt='psql'))
                print("\nWorking' finished !!!")
                run_again = input("Do you want to run the program again? (Y/N): ").upper()
                return run_again
            elif process == "N":
                self.run_queries_with_libraries_check
          
    def execute_query(self, sql_code):
        return pandas_gbq.read_gbq(sql_code, project_id=self.project_id)

    def save_to_csv(self, result_df2, start_date, end_date):
        file_name = f"Data Per Channel Offline or Online {start_date.replace('-', '_')}_to_{end_date.replace('-', '_')}.csv"
        download_folder = os.path.expanduser("~/Downloads")
        file_path = os.path.join(download_folder, file_name)
        result_df2.to_csv(file_path, index=False)
        print(f"Dataframe berhasil disimpan di: {file_path}")
    
    def queries_display(self, start_date, end_date):
        sql_channel = f""" 
        select
          "Summary_Periode" as f0,
          sum(order_net_sales) as net_sales,
          count(order_id) as total_bill,
          sum(case when segment_name = "Delivery" then delivery_fee else 0 end) as delivery_fee,
          sum(case when segment_name in ("Dine-in","Takeaway","Aggregator") then delivery_fee else 0 end) as takeaway_charge
        from
          `phi-gcp-2021.analytics.orders_marketing_orders_t`
        where
          business_date between "{start_date}" and "{end_date}"
        """
        df_display = self.execute_query(sql_channel)
        channel_display = df_display.copy()
        return channel_display

    def run_queries(self, start_date, end_date):
        # SQL Code 1
        sql_code_1 = f"""
        select
          business_date,
          store_code,
          store_name,
          store_type_fcd,
          segment_name,
          is_online,
          sum(order_net_sales) as net_sales,
          count(order_id) as total_bill,
          sum(case when segment_name = "Delivery" then delivery_fee else 0 end) as delivery_fee,
          sum(case when segment_name in ("Dine-in","Takeaway","Aggregator") then delivery_fee else 0 end) as takeaway_charge
        from
          `phi-gcp-2021.analytics.orders_marketing_orders_t`
        where
          business_date between "{start_date}" and "{end_date}"
        group by 
          1,2,3,4,5,6
        order by 
          business_date,
          store_code asc;  
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


# History Bill
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
            print(Fore.GREEN + "Program Name : Get Data History Bill Per-Outlet by Date")
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
            return run_again

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

# class history bill per item
class QueryExecutor_HistoryBill_Item_PerOutlet:
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
            print(Fore.GREEN + "Program Name : Get Data Bill per Item")
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
        file_name = f"monthly DATA History Bill Per-product Store Code {store_code} {start_date.replace('-', '_')}_to_{end_date.replace('-', '_')}.csv"
        download_folder = os.path.expanduser("~/Downloads")
        file_path = os.path.join(download_folder, file_name)
        result_df2.to_csv(file_path, index=False)
        print(f"Dataframe berhasil disimpan di: {file_path}")
    
    def run_queries(self, start_date, end_date, store_code):
        # SQL Code 1
        sql_code_1 = f"""
        select
            o.store_code,
            0.store_name,
            o.order_number,
            FORMAT_DATETIME('%m/%d/%Y %I:%M:%S %p', DATETIME(TIMESTAMP(CONCAT(o.business_date, ' ', o.order_time)))) AS order_time,
            od.product_name,
            od.sub_price,
            od.quantity,
            od.gross_sales as sub_total,
            (od.gross_sales + o.tax) as after_tax,
            o.discount,
        from
            `phi-gcp-2021.analytics.orders_staging` as o
            left join
            `phi-gcp-2021.analytics.order_details_staging` as od
        on
            o.order_id = od.order_id
        where
            o.business_date between "{start_date}" and "{end_date}"
            and o.store_code = "{store_code}";
          """
        df1 = self.execute_query(sql_code_1)

        print("Merging... :)")
        print("="*70)
        
        for _ in tqdm(range(len(df1)), desc="Merging Progress", unit="row"):
            time.sleep(0.001)

        for _ in tqdm(range(20), desc="Merging Progress", unit="second"):
            time.sleep(0.01)
        print("="*70)
        print("Finished !!!")
        
        result_df = df1.copy()
        self.save_to_csv(result_df, start_date, end_date, store_code)
        return result_df
    

# class get data meal outlet
class QueryExecutor_MEAL_Sales_Mix_Usage:
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
            print(Fore.GREEN + "Program Name : MEAL Inventory Sales Mix Usage")
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
            return run_again

    def execute_query(self, sql_code):
        return pandas_gbq.read_gbq(sql_code, project_id=self.project_id)

    def save_to_csv(self, result_df, start_date, end_date):
        file_name = f"Meal Sales Mix Usage {start_date.replace('-', '_')}_to_{end_date.replace('-', '_')}.csv"
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
            and sku_name like "%Meal%"
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
            and sku_name like "%Meal%"
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


# OC Remark BOG
class QueryExecutor_OC_remark_BOG:
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
            print(Fore.GREEN + "Program Name : OC Remark BOG")
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
            return run_again

    def execute_query(self, sql_code):
        return pandas_gbq.read_gbq(sql_code, project_id=self.project_id)

    def save_to_csv(self, result_df, start_date, end_date):
        file_name = f"OC Remark BOG {start_date.replace('-', '_')}_to_{end_date.replace('-', '_')}.csv"
        download_folder = os.path.expanduser("~/Downloads")
        file_path = os.path.join(download_folder, file_name)
        result_df.to_csv(file_path, index=False)
        print(f"Dataframe berhasil disimpan di: {file_path}")
    
    def run_queries(self, start_date, end_date):
        # SQL Code 1
        sql_code_1 = f"""
        select  
            p.store_code,
            p.store_name,
            p.business_date,
            p.order_number,
            p.payment_code_name,
            p.remark,
            o.product_total_cost as ammount
        from
            `phi-gcp-2021.analytics.order_payment` as p
        inner join
            `phi-gcp-2021.analytics.orders` as o
        on
            p.order_code = o.order_code
        where
            p.business_date between "{start_date}" and "{end_date}"
            and p.is_oc = "Y"
            and p.remark like "%BOG%"
        order by
            p.store_code,
            p.business_date
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

# class get data per segment
class QueryExecutor_PerSegment:
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
            print(Fore.GREEN + "Program Name : Get Data Per Segment All Outlet by Date")
            print("=" * 70)
            
            self.get_password()
            
            if self.password == self.default_password:
                print("Authentication successful!\n")
            else:
                print("Authentication failed. Please check your password.")
                break
            
            start_date = input("Enter Start Date Format YYYY-MM-DD:")
            end_date = input("Enter End Date Format YYYY-MM-DD:")
            result_df = self.run_queries(start_date, end_date)
            
            result_df_selected = result_df.iloc[:, 0:8]

            headers_df = result_df_selected.columns.tolist()
            
            print("\nResult Original DataFrame Preview:")
            print(tabulate(result_df_selected.head(32), headers_df, tablefmt='psql'))
            print("\nWorking' finished !!!")
            run_again = input("Do you want to run the program again? (Y/N): ").upper()
            return run_again
            
    def execute_query(self, sql_code):
        return pandas_gbq.read_gbq(sql_code, project_id=self.project_id)

    def save_to_csv(self, result_df2, start_date, end_date):
        file_name = f"Data Sales Per Segments {start_date.replace('-', '_')}_to_{end_date.replace('-', '_')}.csv"
        download_folder = os.path.expanduser("~/Downloads")
        file_path = os.path.join(download_folder, file_name)
        result_df2.to_csv(file_path, index=False)
        print(f"Dataframe berhasil disimpan di: {file_path}")
    
    def run_queries(self, start_date, end_date):
        # SQL Code 1
        sql_code_1 = f"""
        select
          business_date,
          store_code,
          store_name,
          store_type_fcd as store_concept,
          segment_name,
          sum(order_net_sales) as net_sales,
          count(order_id) as total_bill,
          sum(case when segment_name = "Delivery" then delivery_fee else 0 end) as delivery_fee,
          sum(case when segment_name in ("Dine-in","Takeaway","Aggregator") then delivery_fee else 0 end) as takeaway_charge
        from
          `phi-gcp-2021.analytics.orders_marketing_orders_t`
        where
          business_date between "{start_date}" and "{end_date}"
        group by 
          1,2,3,4,5
        order by 
          business_date,
          store_code asc;  
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


# main function
def main():
    project_id = 'phi-gcp-2021'
    
    while True:
        print("INITIALS : Vrooh933")
        print("PROJECT : GOOGLE BIG QUERY DATASET REATRIVAL FOR ACCOUNTING PURPOSE")
        print("DEVELOPER : M AFIF RIZKY A")
        print("VERSION : PTYHON 9.3.3")
        print("="*30)
        print("Daftar Program yang Tersedia:")
        print("")
        print("*"*30)
        print("GET DATA SALES")
        print("*"*30)
        print(Fore.GREEN + "1. Get data sales harian by date" + Style.RESET_ALL)
        print("↳ NOTE: Get data sales per-tanggal seluruh outlet termasuk detail payment. Untuk data closing sales team")
        print(Fore.GREEN + "2. Get data sales summary by date" + Style.RESET_ALL)
        print("↳ NOTE: Get total sales keseluruhan tanggal seluruh outlet termasuk detail payment. Untuk data closing sales team")
        print(Fore.GREEN + "3. Get data sales harian per-outlet by date"+ Style.RESET_ALL)
        print("↳ NOTE: Get data sales 1 tanggal per 1 outlet termasuk detail payment. Untuk data closing sales team jika ada yang terlewat")
        print(Fore.GREEN + "4. Get data sales Summary Lite"+ Style.RESET_ALL)
        print("↳ NOTE: Get data sales summary secara total tanpa detail payment")
        print(Fore.GREEN + "5. Get data sales per channel offline / online by date"+ Style.RESET_ALL)
        print("↳ NOTE: Get data sales per-channel. Untuk data weekly sales team")
        print(Fore.GREEN + "6. Get data sales per segment by date"+ Style.RESET_ALL)
        print("↳ NOTE: Get data sales per segment. Untuk data weekly sales team")
        print(Fore.GREEN + "7. Get data sales history bill per-outlet by date" + Style.RESET_ALL)
        print("↳ NOTE: get data history bill summary per 1 outlet by tanggal. Untuk data tax atau kebutuhan sales")
        print(Fore.GREEN + "8. Get data sales history bill by item per-outlet by date" + Style.RESET_ALL)
        print("↳ NOTE: get data history bill hingga detail product")
        print("")
        print("*"*30)
        print("GET DATA DISCOUNT")
        print("*"*30)
        print(Fore.GREEN + "9. Get data discount by date" + Style.RESET_ALL)
        print("↳ NOTE: Get data discount seluruh outlet berdasarkan tanggal. Untuk data recon sales team")
        print(Fore.GREEN + "10. Get data discount per-outlet by date" + Style.RESET_ALL)
        print("↳ NOTE: Get data discount 1 outlet berdasarkan tanggal. Untuk data recon sales team")
        print(Fore.GREEN + "11. Get data discount per-kode by date" + Style.RESET_ALL)
        print("↳ NOTE: Get data discount per kode discount untuk seluruh outlet berdasarkan tanggal")
        print("")
        print("*"*30)
        print("GET DATA PAYMENT")
        print("*"*30)
        print(Fore.GREEN + "12. Get data payment per-kode payment by date" + Style.RESET_ALL)
        print("↳ NOTE: get data payment berdasarkan kode payment untuk seluruh outlet berdasarkan tanggal")
        print(Fore.GREEN + "13. Get data payment per-outlet by date" + Style.RESET_ALL)
        print("↳ NOTE: Get data seluruh payment untuk satu outlet berdasarkan tanggal")
        print("")
        print("*"*30)
        print("GET DATA OC")
        print("*"*30)
        print(Fore.GREEN + "14. Get data OC per-kode OC by date" + Style.RESET_ALL)
        print("↳ NOTE: Get data OC untuk 1 kode OC seluruh outlet berdasarkan tanggal")
        print(Fore.GREEN + "15. Get data all OC outlet by date" + Style.RESET_ALL)
        print("↳ NOTE: Get data seluruh OC seluruh outlet berdasarkan tanggal")
        print(Fore.GREEN + "16. Get data all OC Summary by date" + Style.RESET_ALL)
        print("↳ NOTE: Get data summary all OC untuk seluruh outlet berdasarkan tanggal")
        print("")
        print("*"*30)
        print("GET DATA COST CONTROL")
        print("*"*30)
        print(Fore.GREEN + "17. Get Data Inventory ALL Sales Mix Usage" + Style.RESET_ALL)
        print(Fore.GREEN + "18. Get Data Meal Inventory Usage" + Style.RESET_ALL)
        print(Fore.GREEN + "19. Get Data OC Remark BOG" + Style.RESET_ALL)
        print("")
        choice = input("Masukkan nomor program yang ingin dijalankan: ")

        if choice == "1":
            executor = QueryExecutor_harian(project_id)
        elif choice == "2":
            executor = QueryExecutor_summary(project_id)
        elif choice == "3":
            executor = QueryExecutor_harian_peroutlet(project_id)
        elif choice == "4":
            executor = QueryExecutor_Summary_Lite(project_id)
        elif choice == "5":
            executor = QueryExecutor_PerChannel(project_id)
        elif choice == "6":
            executor = QueryExecutor_PerSegment(project_id)
        elif choice == "7":
            executor = QueryExecutor_HistoryBill_PerOutlet(project_id)
        elif choice == "8":
            executor = QueryExecutor_HistoryBill_Item_PerOutlet(project_id)
        elif choice == "9":
            executor = QueryExecutor_Discount_Monthly(project_id)
        elif choice == "10":
            executor = QueryExecutor_Discount_PerOutlet(project_id)
        elif choice == "11":
            executor = QueryExecutor_Discount_PerKode(project_id)
        elif choice == "12":
            executor = QueryExecutor_Payment_PerKode(project_id)
        elif choice == "13":
            executor = QueryExecutor_Payment_PerOutlet(project_id)
        elif choice == "14":
            executor = QueryExecutor_OC_PerKode(project_id)
        elif choice == "15":
            executor = QueryExecutor_OC_PerBulan(project_id)
        elif choice == "16":
            executor = QueryExecutor_OC_Summary(project_id)
        elif choice == "17":
            executor = QueryExecutor_Sales_Mix_Usage(project_id)
        elif choice == "18":
            executor = QueryExecutor_MEAL_Sales_Mix_Usage(project_id)
        elif choice == "19":
            executor = QueryExecutor_OC_remark_BOG(project_id)
        else:
            print("Pilihan tidak valid")
            break
        print("OK! Executing...")
        run_again = executor.run_queries_with_libraries_check()
        
        # Memeriksa apakah pengguna ingin menjalankan program lagi
        if run_again != 'Y':
            print("Exiting program...")
            break

if __name__ == "__main__":
    main()


