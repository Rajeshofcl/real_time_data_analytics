from kafka import KafkaConsumer
import pandas as pd
import json
import snowflake.connector as sfc
import os

# Consumer Creation
try:

    bootstrap_servers = ['localhost:29092']
    topic_name = 'source.kafka.customer'

    consumer = KafkaConsumer(
        topic_name, 
        auto_offset_reset = 'earliest', 
        bootstrap_servers = bootstrap_servers,
        group_id = 'ecommerce_streaming_customer'
        )

except Exception as e:
    print("Error: ", e)

#Snowflake connection
def sfc_connection(user, password, account, warehouse, database, schema):
    try:
        conn = sfc.connect(
            user = user,
            password = password,
            account = account,
            warehouse = warehouse,
            database = database,
            schema = schema
        )

        return conn #returning connection
    except sfc.Error as e:
        print("Error: Unable to connect with PostgreSQL database. Please check the connection.")
        print(e)
        return None

def ingestion(values, conn):
    cursor = conn.cursor()
    
    print('\n\n\n',values,'\n\n\n')

    try:
        # Spitting the dictionary into values and assigning variable, so that we can use it as insert or update or delete value in snowflake query
        # Also creating variables for target location, such as database name, schema cname and table name.
        # database name and schema names are already pushed to envirnmental variable for security purposes. 
        # defining del_or_not variable to identify the message is to delete the record in snowflake or to other transaction in snowflake
        customerid = values['CUSTOMER_ID']
        customer_unique_id = values['CUSTOMER_UNIQUE_ID']
        firstname = values['FIRST_NAME']
        lastname = values['LAST_NAME']
        phone = values['PHONE']
        email = values['EMAIL']
        customer_zip_code_prefix = values['CUSTOMER_ZIP_CODE_PREFIX']
        customer_city = values['CUSTOMER_CITY']
        customer_state = values['CUSTOMER_STATE']

        target_database = "ECOMMERCE"
        target_schema = "KAFKA"
        target_table = "CUSTOMER"
        
        # in order to decide that we need to insert or update or delete in Snowlake table, weneed to fine the record is already present in that table or not, 
        # For that, querying the count(*) from customer table where customerid is received customerid from the message
        # cursor.execute(f'SELECT COUNT(*) FROM {target_database}.{target_schema}.{target_table} WHERE customerid in ({customerid})')
        # result = cursor.fetchone()[0]

        #Defining Logic to deciode the process Insert or Update or Delete
        # 1. If the record is already present in SNowflake, and if it's a delete transaction,THEN deleting the record from snowflake
        # 2. If the record is already present in Snowflake, and if it's not a delete transaction,THEN update the values using the primary key customerid.
        # 3. If the record is not present in snowflake, and if it's not a delete transaction, THEN insert the record to snowflake table

        # if result != 0 and del_or_not == 'true':
        #     del_query = f'DELETE FROM {target_database}.{target_schema}.{target_table} WHERE CUSTOMERID IN ({customerid});'
        #     cursor.execute(del_query)
        #     print(f"Record {customerid} has been deleted successfully.")
        # elif result != 0 and del_or_not == 'false':
        #     update_query = f"UPDATE {target_database}.{target_schema}.{target_table} SET FIRSTNAME = '{firstname}', LASTNAME = '{lastname}', COMPANY = '{company}', ADDRESS = '{address}', CITY = '{city}', STATE = '{state}', COUNTRY = '{country}', POSTALCODE = '{postalcode}', PHONE = '{phone}', FAX = '{fax}', EMAIL = '{email}' WHERE CUSTOMERID = '{customerid}'"
        #     cursor.execute(update_query)
        #     print(f"Record {customerid} has been updated successfully.")
        # elif result == 0 and del_or_not == 'false':

        insert_query = f"""INSERT INTO {target_database}.{target_schema}.{target_table} 
                            (
                            CUSTOMER_ID,
                            CUSTOMER_UNIQUE_ID,
                            FIRST_NAME,
                            LAST_NAME,
                            PHONE,
                            EMAIL,
                            CUSTOMER_ZIP_CODE_PREFIX,
                            CUSTOMER_CITY,
                            CUSTOMER_STATE) 
                        VALUES 
                            (
                            {customerid},
                            '{customer_unique_id}',
                            '{firstname}',
                            '{lastname}',
                            '{phone}',
                            '{email}',
                            '{customer_zip_code_prefix}',
                            '{customer_city}',
                            '{customer_state}'
                            )"""
        cursor.execute(insert_query)
        print(f"Record {customerid} has been inserted successfully.")
        conn.commit()
    except sfc.Error as e: 
        print(f"Error: {e}")


def ingestion_customer_orders(values, conn):
    cursor = conn.cursor()

    try:
        order_id = values['ORDER_ID']
        customer_id = values['CUSTOMER_ID']
        order_date = values['ORDER_DATE']
        total_amount = values['TOTAL_AMOUNT']
        shipping_fee = values['SHIPPING_FEE']
        payment_status = values['PAYMENT_STATUS']
        delivery_date = values['DELIVERY_DATE']
        order_status = values['ORDER_STATUS']

        target_database = "ECOMMERCE"
        target_schema = "KAFKA"
        target_table = "CUSTOMER_ORDERS"

        insert_query = f"""INSERT INTO {target_database}.{target_schema}.{target_table} 
                            (
                            ORDER_ID,
                            CUSTOMER_ID,
                            ORDER_DATE,
                            TOTAL_AMOUNT,
                            SHIPPING_FEE,
                            PAYMENT_STATUS,
                            DELIVERY_DATE,
                            ORDER_STATUS
                            ) 
                        VALUES 
                            (
                            {order_id},
                            {customer_id},
                            '{order_date}',
                            {total_amount},
                            {shipping_fee},
                            '{payment_status}',
                            '{delivery_date}',
                            '{order_status}'
                            )"""
        cursor.execute(insert_query)
        print(f"Record {order_id} has been inserted successfully.")
        conn.commit()
    except sfc.Error as e:
        print(f"Error: {e}")


def ingestion_customer_purchases(values, conn):
    cursor = conn.cursor()

    try:
        purchase_id = values['PURCHASE_ID']
        customer_id = values['CUSTOMER_ID']
        product_id = values['PRODUCT_ID']
        purchase_date = values['PURCHASE_DATE']
        purchase_amount = values['PURCHASE_AMOUNT']
        quantity = values['QUANTITY']
        payment_method = values['PAYMENT_METHOD']
        shipping_address = values['SHIPPING_ADDRESS']
        purchase_status = values['PURCHASE_STATUS']

        target_database = "ECOMMERCE"
        target_schema = "KAFKA"
        target_table = "CUSTOMER_PURCHASES"

        insert_query = f"""INSERT INTO {target_database}.{target_schema}.{target_table} 
                            (
                            PURCHASE_ID,
                            CUSTOMER_ID,
                            PRODUCT_ID,
                            PURCHASE_DATE,
                            PURCHASE_AMOUNT,
                            QUANTITY,
                            PAYMENT_METHOD,
                            SHIPPING_ADDRESS,
                            PURCHASE_STATUS
                            ) 
                        VALUES 
                            (
                            {purchase_id},
                            {customer_id},
                            {product_id},
                            '{purchase_date}',
                            {purchase_amount},
                            {quantity},
                            '{payment_method}',
                            '{shipping_address}',
                            '{purchase_status}'
                            )"""
        cursor.execute(insert_query)
        print(f"Record {purchase_id} has been inserted successfully.")
        conn.commit()
    except sfc.Error as e:
        print(f"Error: {e}")

def ingestion_product_categories(values, conn):
    cursor = conn.cursor()

    try:
        category_id = values['CATEGORY_ID']
        category_name = values['CATEGORY_NAME']
        subcategory_name = values['SUBCATEGORY_NAME']
        product_id = values['PRODUCT_ID']
        product_price = values['PRODUCT_PRICE']
        discount = values['DISCOUNT']
        availability_status = values['AVAILABILITY_STATUS']

        target_database = "ECOMMERCE"
        target_schema = "KAFKA"
        target_table = "PRODUCT_CATEGORIES"

        insert_query = f"""INSERT INTO {target_database}.{target_schema}.{target_table} 
                            (
                            CATEGORY_ID,
                            CATEGORY_NAME,
                            SUBCATEGORY_NAME,
                            PRODUCT_ID,
                            PRODUCT_PRICE,
                            DISCOUNT,
                            AVAILABILITY_STATUS
                            ) 
                        VALUES 
                            (
                            {category_id},
                            '{category_name}',
                            '{subcategory_name}',
                            {product_id},
                            {product_price},
                            {discount},
                            '{availability_status}'
                            )"""
        cursor.execute(insert_query)
        print(f"Record {product_id} has been inserted successfully.")
        conn.commit()
    except sfc.Error as e:
        print(f"Error: {e}")

def ingestion_inventory_management(values, conn):
    cursor = conn.cursor()

    try:
        inventory_id = values['INVENTORY_ID']
        product_id = values['PRODUCT_ID']
        stock_available = values['STOCK_AVAILABLE']
        stock_ordered = values['STOCK_ORDERED']
        stock_sold = values['STOCK_SOLD']
        last_restock_date = values['LAST_RESTOCK_DATE']
        supplier_id = values['SUPPLIER_ID']
        warehouse_location = values['WAREHOUSE_LOCATION']
        stock_threshold = values['STOCK_THRESHOLD']

        target_database = "ECOMMERCE"
        target_schema = "KAFKA"
        target_table = "INVENTORY_MANAGEMENT"

        insert_query = f"""INSERT INTO {target_database}.{target_schema}.{target_table} 
                            (
                            INVENTORY_ID,
                            PRODUCT_ID,
                            STOCK_AVAILABLE,
                            STOCK_ORDERED,
                            STOCK_SOLD,
                            LAST_RESTOCK_DATE,
                            SUPPLIER_ID,
                            WAREHOUSE_LOCATION,
                            STOCK_THRESHOLD
                            ) 
                        VALUES 
                            (
                            {inventory_id},
                            {product_id},
                            {stock_available},
                            {stock_ordered},
                            {stock_sold},
                            '{last_restock_date}',
                            {supplier_id},
                            '{warehouse_location}',
                            {stock_threshold}
                            )"""
        cursor.execute(insert_query)
        print(f"Record {inventory_id} has been inserted successfully.")
        conn.commit()
    except sfc.Error as e:
        print(f"Error: {e}")


def ingestion_social_media_interactions(values, conn):
    cursor = conn.cursor()

    try:
        interaction_id = values['INTERACTION_ID']
        customer_id = values['CUSTOMER_ID']
        social_media_platform = values['SOCIAL_MEDIA_PLATFORM']
        post_id = values['POST_ID']
        interaction_type = values['INTERACTION_TYPE']
        interaction_date = values['INTERACTION_DATE']
        sentiment_score = values['SENTIMENT_SCORE']
        engagement_score = values['ENGAGEMENT_SCORE']

        target_database = "ECOMMERCE"
        target_schema = "KAFKA"
        target_table = "SOCIAL_MEDIA_INTERACTIONS"

        insert_query = f"""INSERT INTO {target_database}.{target_schema}.{target_table} 
                            (
                            INTERACTION_ID,
                            CUSTOMER_ID,
                            SOCIAL_MEDIA_PLATFORM,
                            POST_ID,
                            INTERACTION_TYPE,
                            INTERACTION_DATE,
                            SENTIMENT_SCORE,
                            ENGAGEMENT_SCORE
                            ) 
                        VALUES 
                            (
                            {interaction_id},
                            {customer_id},
                            '{social_media_platform}',
                            '{post_id}',
                            '{interaction_type}',
                            '{interaction_date}',
                            {sentiment_score},
                            {engagement_score}
                            )"""
        cursor.execute(insert_query)
        print(f"Record {interaction_id} has been inserted successfully.")
        conn.commit()
    except sfc.Error as e:
        print(f"Error: {e}")


def ingestion_website_activity(values, conn):
    cursor = conn.cursor()

    try:
        activity_id = values['ACTIVITY_ID']
        customer_id = values['CUSTOMER_ID']
        page_visited = values['PAGE_VISITED']
        visit_timestamp = values['VISIT_TIMESTAMP']
        session_id = values['SESSION_ID']
        referrer_source = values['REFERRER_SOURCE']
        device_type = values['DEVICE_TYPE']
        browser_type = values['BROWSER_TYPE']
        time_spent = values['TIME_SPENT']

        target_database = "ECOMMERCE"
        target_schema = "KAFKA"
        target_table = "WEBSITE_ACTIVITY"

        insert_query = f"""INSERT INTO {target_database}.{target_schema}.{target_table} 
                            (
                            ACTIVITY_ID,
                            CUSTOMER_ID,
                            PAGE_VISITED,
                            VISIT_TIMESTAMP,
                            SESSION_ID,
                            REFERRER_SOURCE,
                            DEVICE_TYPE,
                            BROWSER_TYPE,
                            TIME_SPENT
                            ) 
                        VALUES 
                            (
                            {activity_id},
                            {customer_id},
                            '{page_visited}',
                            '{visit_timestamp}',
                            '{session_id}',
                            '{referrer_source}',
                            '{device_type}',
                            '{browser_type}',
                            {time_spent}
                            )"""
        cursor.execute(insert_query)
        print(f"Record {activity_id} has been inserted successfully.")
        conn.commit()
    except sfc.Error as e:
        print(f"Error: {e}")



def initiate_kafka(conn):
    print(consumer.topics())

    print("\n Kafka consumer is now actively listening for incoming messages from Customer table.\n")
    # Gettings the messages from consumer and getting the value content of the message.
    # Once we got the values in bytes format, decoding the bytes to string. 
    # Once it's done, loading the string to dictionary and passing the value to ingestion function to ingest into snowflake
    
    for message in consumer:
        print("Ingestion Started")

        print(message)

        str_value = message.value.decode('utf-8')
        msg = json.loads(str_value)

        print(msg)

        ingestion(msg, conn)

#Defining Environmental Variables
# user = os.environ.get("sfc_user")
# password = os.environ.get("sfc_pwd")
# account = os.environ.get("sfc_account")
# warehouse = os.environ.get("sfc_warehouse")
# database = os.environ.get("sfc_database")
# schema = os.environ.get("sfc_schema")


user = "RAJESHK"
password = "Password@258036"
account = "yp34405.ap-southeast-1"
warehouse = "COMPUTE_WH"
database = "ECOMMERCE"
schema = "KAFKA"

conn = sfc_connection(user, password, account, warehouse, database, schema) #received Snowflake connection

initiate_kafka(conn)

    
    


