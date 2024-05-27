import pandas_gbq
import pandas as pd
import numpy as np
from tqdm import tqdm
import time
import os
from tabulate import tabulate
from colorama import Fore
import getpass 

class QueryExecutor:
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
            print(Fore.GREEN + "Program Name : Get Data Sales Big Query Summary Harian")
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
            print("\nWorking finished !!!")
            run_again = input("Do you want to run the program again? (Y/N): ").upper()

            if run_again != 'Y':
                break

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
            'Kredivo', 'Voucher Sodexo/Pluxee', 'Harian / Bulanan', 'Nama File',
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
          SUM(CASE WHEN segment = "Dine-in" THEN discount END) as Discount_Food_Dine_In,
          SUM(CASE WHEN segment = "Takeaway" THEN discount END) as Discount_Food_Takeaway,
          SUM(CASE WHEN segment = "Delivery" THEN discount END) as Discount_Food_Delivery,
          SUM(CASE WHEN segment = "Aggregator" THEN discount END) as Discount_Food_Aggregator,
          0 as Discount_BVRG_Eat_In,
          0 as Discount_BVRG_Dine_In,
          0 as Discount_BVRG_Takeaway,
          0 as Discount_BVRG_Delivery,
          0 as Discount_BVRG_Aggregator,

          --total delfee--
          SUM(CASE WHEN segment = "Delivery" THEN delivery_fee END) as Delivery_Cost_Total,
          
          --total takeaway charge--
          0 as Takaway_Cost_Eat_In,
          SUM(CASE WHEN segment = "Dine-in" THEN delivery_fee END) as Takeaway_Cost_Dine_In,
          SUM(CASE WHEN segment = "Takeaway" THEN delivery_fee END) as Takeaway_Cost_Take_Away,
          SUM(CASE WHEN segment = "Aggregator" THEN delivery_fee END) as Takeaway_Cost_Aggregator,

          --total tax--
          0 as Restaurant_Tax_Eat_In,
          SUM(CASE WHEN segment = 'Dine-in' THEN tax END) as Restaurant_Tax_Dine_In,
          SUM(CASE WHEN segment = 'Takeaway' THEN tax END) as Restaurant_Tax_Takeaway,
          SUM(CASE WHEN segment = 'Delivery' THEN tax END) as Restaurant_Tax_Delivery,
          SUM(CASE WHEN segment = 'Aggregator' THEN tax END) as Restaurant_Tax_Aggregator,
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
          SUM(CASE WHEN payment_code = 105 THEN payment_value ELSE 0 END) as VOUCHER_SODEXO_PLUXEE
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

project_id = 'phi-gcp-2021'
query_executor = QueryExecutor(project_id)
query_executor.run_queries_with_libraries_check()


# In[ ]:




