

Real-Time Data Streaming from PostgreSQL to Snowflake using Kafka


Overview:

This project aims to achieve real-time data streaming from PostgreSQL to Snowflake using Kafka. By leveraging Kafka as a messaging system, Debezium for change data capture (CDC), and Python for data processing.

Technologies Used:
 * PostgreSQL (v)
 * Snowflake (v)
 * Kafka (v)
 * Debezium (v)
 * Python (v)

Before you begin:
Make sure to setup the below requirements before beginning this project setup.

  1. PostgreSQL 

Installation guide: https://www.w3schools.com/postgresql/postgresql_install.php

  2. Make sure wal_level of your PostgreSQL is "logical", so that it will allow us to capture the changes. And create a logical replication

Guide: https://hevodata.com/learn/postgresql-logical-replication/

  2. Docker Desktop 

Installation guide: https://docs.docker.com/desktop/install/windows-install/

  3. Github

Setup guide: https://docs.github.com/en/repositories/creating-and-managing-repositories/quickstart-for-repositories

  4. Connecting Github to your IDE - In this project, I've used VS Code

Guide: https://docs.github.com/en/codespaces/developing-in-a-codespace/using-github-codespaces-in-visual-studio-code

  5. Snowflake setup guide:

Guide: https://docs.snowflake.com/en/user-guide-getting-started

Setup Instructions

1. Start Docker Engine
 
2. Clone the repository using terminal

git clone git@github.com:ranjith-ofcl/kafka_data_streaming.git

3. Navigate to the Project directory

cd kafka_data_streaming/

4. create a new terminal and start the Docker containers

docker-compose up -d

docker-compose file has all the required images and container informations to run this project efficiently.

5. Once the Docker images are up and running make sure they are responding as expected using your browser

    * ZOOKEEPER_CLIENT_PORT: localhost:2181
    * KAFKA: localhost:29092
    * DEBEZIUM: localhost:8083
    * SCHEMA-REGISTRY: localhost:8081
    * KAFKA-MANAGER: 9000

In this project, it's not necessary to interact with Zookeeper port, kafka port or schema-registry port. We only need Kafka-manager and Debezium, so if other ports are not responding, it will not impact the process. Make sure the container is running fine in Docker Desktop application

6. Browse to Kafka-manager port (localhost:9000). Here you can see list of clusters (there will be none as we haven't created yet), click on create cluster and create a cluster.
    * Name your cluster
    * Cluster Zookeeper Hosts - "zookeeper:2181" #Our zookeeper is running on localhost:2181, and kafka is depended on zookeeper
    * Enable "JMX Polling" - to poll the consumer messages
    * Enable "Poll Consumer Information" - we can obtain the current offset like "latest" or "earliest".
    * Finally click on "Save"

    You have successfully created your Kafka cluster now. You can click on "Go to Cluster view" to check information about cluster, topic and more.
    Once the cluster is created, Kafka is ready to receive messages from the producer. Let's setup the producer, in our case, PostgreSQL

7. Create a database as per your requirement and two tables as the data-model of this project.

Table-1: CREATE TABLE public.customer (customerid TEXT PRIMARY KEY, firstname TEXT, lastname TEXT, company TEXT, address TEXT, city TEXT, state TEXT, country TEXT, postalcode TEXT, phone TEXT, fax TEXT, email TEXT);

Table-2: CREATE TABLE public.invoice (invoiceid TEXT, customerid TEXT, invoicedate TEXT, billingaddress TEXT, billingcity TEXT, billingstate TEXT, billingcountry TEXT, billingpostalcode TEXT, total INTEGER, PRIMARY KEY (invoiceid, customerid));

8. As we have created database and tables in postgreSQL, create both tables exactly as same in snowflake

9. Now source, kafka streaming tool and the destination is ready. Let's create a connection from debezium to PostgreSQL, so we can capture the changes of our database and send messages to kafka

10. For this, we will need to create a Debezium-postgres connector in RestAPI creation in post method, you can use preferred application or webpage to create this, and the connector code is in "debezium.config" file, copy and paste and create a connector

11. Make sure the connector is created and working fine now by visiting to "localhost:8083/connectors". You can view your connector name here.

12. We have everything setup and ready to stream. Run the below two scripts which are available in data_streaming_scripts folder, so kafka will begin to listen for messages from postgres.
    * customer_consumer.py
    * invoice_consumer.py

13. Now kafka is listening for messages and our script will capture those messages and ingest the data of the message to snowflake in real-time.

14. Try makeing transactions on your PostgreSQL tables (Insert, Update or Delete), you can also run the pg_data_loader.py script which will load data to our tables from the csv file which are saved in sample_data folder.

15. Visit snowflake and make sure the data are present.

Data from PostgreSQL is now streaming to Snowflake in real-time, and the real-time data can be used for data science, data analytics, data vishualization and many more purposes.


ADDITIONAL NOTES: MODIFY THE CONFIGURATION FILES AS NEEDED TO MATCH YOUR ENVIRONMENT SETUP.




------ Contributor

Ranjith R

ranjith.ofcl@gmail.com

https://www.linkedin.com/in/ranjith-r-a852b0165/










