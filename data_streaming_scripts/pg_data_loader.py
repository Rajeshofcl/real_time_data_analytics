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

def load_customer_data(conn):
    try:
        cur = conn.cursor()
        print('\n\n\n',cur,'\n\n\n')
        df = pd.read_csv(r'D:\Project\real_time_data_analytics\sample_data\Customer.csv') #Loading the data from "customer.csv"
        print(df)

        #Iterate each rows of values in dataframe df
        for index, value in df.iterrows():
            #PostgreSQL query to insert row values (genreid and genrename) into the table "genre"
            query = """
                        INSERT INTO kafka.customer(
                            "CustomerId", "FirstName", "LastName", "Company", "Address", "City", "State", "Country", "PostalCode", "Phone", "Fax", "Email"
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """
            cur.execute(query, tuple(value))
            conn.commit() #Commiting the changes
            conn.close
        print("Data Loaded Successfully.")
    except Exception as e:
        print("Error: ", e)

def load_invoice_data(conn):
    try:
        cur = conn.cursor() 
        df = pd.read_csv(r'D:\Project\real_time_data_analytics\sample_data\Invoice.csv') #Loading the data from "customer.csv"
        print(df)
        #Iterate each rows of values in dataframe df
        for index, value in df.iterrows():
            #PostgreSQL query to insert row values (genreid and genrename) into the table "genre"
            query = """
                        INSERT INTO kafka.invoice(
                            "InvoiceId", "CustomerId", "InvoiceDate", "BillingAddress", "BillingCity", "BillingState", "BillingCountry", "BillingPostalCode", "Total"
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """
            cur.execute(query, tuple(value))
            conn.commit() #Commiting the changes
            conn.close
        print("Data Loaded Successfully.")
    except Exception as e:
        print("Error: ", e)

# dbname = os.environ.get("postgres_to_snowflake_dbname")
# user = os.environ.get("postgres_user")
# password = os.environ.get("postgres_user_pwd")
# host = os.environ.get("postgres_host")
# port = os.environ.get("postgres_port")


dbname = "ecommerce_db"
user = "postgres"
password = "12341234"
host = "127.0.0.1"
port = "5432"

conn = psg_connection(dbname, user, password, host, port)
load_customer_data(conn)
# load_invoice_data(conn)



