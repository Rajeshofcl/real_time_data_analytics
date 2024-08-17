import snowflake.connector


user = "RAJESHK"
password = "Password@258036"
account = "wk91667-mjgdjan"
warehouse = "COMPUTE_WH"
database = "ECOMMERCE_DB"
schema = "KAFKA"

# Define your Snowflake connection parameters

# Establish a connection to Snowflake
conn = snowflake.connector.connect(
    user="RAJESHK",
    password="Password@258036",
    # account="https://yp34405.ap-southeast-1.snowflakecomputing.com",
    account='yp34405.ap-southeast-1',
    # region="ap-southeast-1",
    warehouse="COMPUTE_WH",
    database="ECOMMERCE_DB",
    schema="KAFKA"
)

# Create a cursor object
cursor = conn.cursor()

# Execute a query
query = 'SELECT * FROM ECOMMERCE_DB.INFORMATION_SCHEMA.TABLES'
cursor.execute(query)

# Fetch the results
results = cursor.fetchall()

# print(results.count)

# Print the results
for row in results:
    print(row)

# Close the cursor and connection
cursor.close()
conn.close()
