CREATE TABLE "KAFKA".CUSTOMER
(
 CUSTOMER_ID              varchar(50) NOT NULL,
 CUSTOMER_UNIQUE_ID       varchar(50) NOT NULL,
 FIRST_NAME               varchar(50) NOT NULL,
 LAST_NAME                varchar(50) NOT NULL,
 PHONE                    integer NOT NULL,
 EMAIL                    varchar(50) NOT NULL,
 CUSTOMER_ZIP_CODE_PREFIX integer NOT NULL,
 CUSTOMER_CITY            varchar(50) NOT NULL,
 CUSTOMER_STATE           varchar(50) NOT NULL,
 CONSTRAINT PK_1 PRIMARY KEY ( CUSTOMER_ID, CUSTOMER_UNIQUE_ID )
);



