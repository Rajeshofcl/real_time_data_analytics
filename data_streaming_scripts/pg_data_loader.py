import psycopg2
import pandas as pd
import os

## PostgreSQL Connection
def psg_connection(dbname, user, password, host, port):
    
    try:
        conn = psycopg2.connect(
            dbname = dbname,
            user = user,
            password = password,
            host = host,
            port = port
        )

        return conn
    except psycopg2.Error as e:
        print("Error: Unable to connect with PostgreSQL database. Please check the connection.")
        print(e)
        return None


# dbname = os.environ.get("postgres_to_snowflake_dbname")
# user = os.environ.get("postgres_user")
# password = os.environ.get("postgres_user_pwd")
# host = os.environ.get("postgres_host")
# port = os.environ.get("postgres_port")

def load_customer_orders(conn):
    print("Loading customer_orders...")
    try:
        cur = conn.cursor()
        df = pd.read_csv(r'D:\Project\real_time_data_analytics\ecommerce_dataset\Customer_Orders.csv') #Loading the data from "customer.csv"

        #Iterate each rows of values in dataframe df
        for index, value in df.iterrows():
            #PostgreSQL query to insert row values (genreid and genrename) into the table "genre"
            query = """
                        INSERT INTO kafka.customer_orders(
                            Order_ID,
                            Customer_ID,
                            Order_Date,
                            Total_Amount,
                            Shipping_Fee,
                            Payment_Status,
                            Delivery_Date,
                            Order_Status
                            ) 
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s,%s)
                    """
            
            cur.execute(query, tuple(value))
        
        conn.commit() #Commiting the changes
        cur.close()
        print("Customer Orders Data Loaded Successfully.")
        
    except Exception as e:
        print("Error loading customer orders:", e)

# Load customer purchases data into the database
def load_customer_purchases(conn):
    print("Loading customer_purchases...")
    try:
        cur = conn.cursor()
        df = pd.read_csv(r'D:\Project\real_time_data_analytics\ecommerce_dataset\Customer_Purchases.csv') #Loading the data from "customer.csv"
        
        #Iterate each rows of values in dataframe df
        for index, value in df.iterrows():
            #PostgreSQL query to insert row values (genreid and genrename) into the table "genre"
            query = """
                        INSERT INTO kafka.customer_purchases(
                            purchase_id,
                            customer_id,
                            product_id,
                            purchase_date,
                            purchase_amount,
                            quantity,
                            payment_method,
                            shipping_address,
                            purchase_status
                            ) 
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s,%s,%s)
                    """
            cur.execute(query, tuple(value))

        conn.commit() #Commiting the changes
        cur.close()
        print("Customer Purchases Data Loaded Successfully.")
        
    except Exception as e:
        print("Error loading customer purchases:", e)

# Load customer inventory management data into the database
def load_Inventory_management(conn):
    print("Loading Inventory_management...")
    try:
        cur = conn.cursor()
        df = pd.read_csv(r'D:\Project\real_time_data_analytics\ecommerce_dataset\Inventory_Management.csv') #Loading the data from "customer.csv"
        
        #Iterate each rows of values in dataframe df
        for index, value in df.iterrows():
            #PostgreSQL query to insert row values (genreid and genrename) into the table "genre"
            query = """
                        INSERT INTO kafka.inventory_management(
                            inventory_id,
                            product_id,
                            stock_available,
                            stock_ordered,
                            stock_sold,
                            last_restock_date,
                            supplier_id,
                            warehouse_location,
                            stock_threshold
                            ) 
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s,%s,%s)
                    """
            cur.execute(query, tuple(value))

        conn.commit() #Commiting the changes
        cur.close()
        print("Inventory Management Data Loaded Successfully.")
        
    except Exception as e:
        print("Error loading customer purchases:", e)

# Load customer product_categories  data into the database
def load_product_categories(conn):
    print("Loading product_categories...")
    try:
        cur = conn.cursor()
        df = pd.read_csv(r'D:\Project\real_time_data_analytics\ecommerce_dataset\Product_categories.csv') #Loading the data from "customer.csv"
        
        # Print the first few rows to verify the structure
        print(df.head())

        #Iterate each rows of values in dataframe df
        for index, value in df.iterrows():
            #PostgreSQL query to insert row values (genreid and genrename) into the table "genre"
            query = """
                        INSERT INTO kafka.product_categories(
                            product_id,
                            category_id,
                            category_name,
                            subcategory_name,
                            product_price,
                            discount,
                            availability_status
                            ) 
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s) 
                    """
            cur.execute(query, tuple(value))

        conn.commit() #Commiting the changes
        cur.close()
        print("Product Categories Data Loaded Successfully.")
        
    except Exception as e:
        print("Error loading :", e)
    
# Load Social Media Interactions data into the database
def load_Social_Media_Interactions(conn):
    print("Loading Social_Media_Interactions...")
    try:
        cur = conn.cursor()
        df = pd.read_csv(r'D:\Project\real_time_data_analytics\ecommerce_dataset\Social_Media_Interactions.csv') #Loading the data from "customer.csv"

        #Iterate each rows of values in dataframe df
        for index, value in df.iterrows():
            #PostgreSQL query to insert row values (genreid and genrename) into the table "genre"
            query = """
                        INSERT INTO kafka.social_media_interactions(
                            interaction_id,
                            customer_id,
                            social_media_platform,
                            post_id,
                            interaction_type,
                            interaction_date,
                            sentiment_score,
                            engagement_score
                            ) 
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s,%s)
                    """
            cur.execute(query, tuple(value))

        conn.commit() #Commiting the changes
        cur.close()
        print("Social Media Interactions Data Loaded Successfully.")
        
    except Exception as e:
        print("Error loading customer purchases:", e)

# Load Website_Activity data into the database
def load_Website_Activity(conn):
    print("Loading Website_Activity...")
    try:
        cur = conn.cursor()
        df = pd.read_csv(r'D:\Project\real_time_data_analytics\ecommerce_dataset\Website_Activity.csv') #Loading the data from "customer.csv"

        #Iterate each rows of values in dataframe df
        for index, value in df.iterrows():
            #PostgreSQL query to insert row values (genreid and genrename) into the table "genre"
            query = """
                        INSERT INTO kafka.website_activity(
                            activity_id,
                            customer_id,
                            page_visited,
                            visit_timestamp,
                            session_id,
                            referrer_source,
                            device_type,
                            browser_type,
                            time_spent
                            ) 
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s,%s,%s)
                    """
            cur.execute(query, tuple(value))

        conn.commit() #Commiting the changes
        cur.close()
        print("Website Activity Data Loaded Successfully.")
        
    except Exception as e:
        print("Error loading customer purchases:", e)



dbname = "ecommerce"
user = "postgres"
password = "12341234"
host = "127.0.0.1"
port = "5432"

conn = psg_connection(dbname, user, password, host, port)
load_customer_orders(conn)
load_customer_purchases(conn)
load_Inventory_management(conn)
load_product_categories(conn)
load_Social_Media_Interactions(conn)
load_Website_Activity(conn)
conn.close()
# load_invoice_data(conn)


#load_customer_orders(conn)
