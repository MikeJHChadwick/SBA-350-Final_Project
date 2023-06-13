import pandas as pd
import pyspark as ps
from pyspark.sql import SparkSession
import matplotlib as mp
from pyspark.sql.functions import *

# Creating Spark Session
spark = SparkSession.builder.appName('SBA 350').getOrCreate()
# Reading /loading the Dataset from JSON file, SPARK by defualt infers schema for JSON files; only need
# headers parameter typically for csv files
customer_spark = spark.read.load("cdw_sapp_customer.json", format="json")
branch_spark = spark.read.load("cdw_sapp_branch.json", format= "json")
credit_spark = spark.read.load("cdw_sapp_credit.json", format= "json")

#takes dataframe and one of it's column's and transform it into title case
def tran_cust_title_case(df, column_name):
    return df.withColumn(column_name,initcap(col(column_name)))
#takes dataframe and one of it's column's and transform it into title case
def tran_cust_lower_case(df, column_name):
    return df.withColumn(column_name,lower(col(column_name)))
#takes dataframe and two columns from it and concatenates its respective values
def concat_cust_street_apt(df, col1, col2):
    #concat_ws concatenates multiple string columns 
    return df.withColumn('FULL_STREET_ADDRESS', concat_ws(',', col(col1), col(col2)))
#adds the desired formatting for phone numbers using string slices
def tran_phone_num(df, column_name):
    return df.withColumn(column_name, concat(lit('('), 
                                             substring(col(column_name), 1, 3),
                                             lit(')'),
                                             substring(col(column_name), 4, 3),
                                             lit('-'),
                                             substring(col(column_name), 7, 4))
                                             .cast('string'))
#checks if the branches zip is null and defaults it to 99999, and if it isn't it returns the branches zip
def tran_branch_zip(df):
    return df.withColumn('BRANCH_ZIP', when(col('BRANCH_ZIP').isNull(), 
                                            lit(99999)).otherwise(col('BRANCH_ZIP')))

#match cust state to branch state and slice branch phone 3-5 (last included?) to append to cust phone 
# after 2nd element first left join the cust df and the branch df on their respective states like in sql
custJoinbranch = customer_spark.join(branch_spark, customer_spark['CUST_STATE'] 
                                     == branch_spark['BRANCH_STATE'], 'left')

# Updating the customer phone by appending a sliced portion of the branch phone
customer_fix = custJoinbranch.withColumn('CUST_PHONE', concat(col('CUST_PHONE').substr(1, 2), 
                                                              col('BRANCH_PHONE').substr(3, 3), 
                                                              col('CUST_PHONE').substr(3, 7)))

# concat the day, month, year columns into a TIMEID (YYYYMMDD)
def tran_to_timeid(df, day, month, year):
    # first concat the columns so you can use the to_date function
    #need to lpad the month and day values so not to throw error when parsing to to_date
    #the 2 represents the desired length of the string and the 0 is what we're left-padding with
    date_string = concat(
        col(year),
        lpad(col(month), 2, '0'),
        lpad(col(day), 2, '0'))
    return df.withColumn('TIMEID', to_date(date_string, 'yyyyMMdd'))

#transforming the specified columns, from the mapping document, of the extracted customer df into new df
tran_cust_spark = customer_spark.transform(tran_cust_title_case, 'FIRST_NAME')\
.transform(tran_cust_lower_case, 'MIDDLE_NAME')\
.transform(tran_cust_title_case, 'LAST_NAME')\
.transform(concat_cust_street_apt, 'APT_NO', 'STREET_NAME')\
.drop('STREET_NAME', 'APT_NO')\
.transform(tran_phone_num, 'CUST_PHONE')


#transforming the specified columns, from the mapping document, of the extracted branch df into new df
#just need to transform the branch_zip if the source value is null load default (99999) value else direct 
# move and branch_phone change the format of phone number to (xxx)xxx-xxxx
tran_branch_spark = branch_spark.transform(tran_branch_zip)\
.transform(tran_phone_num, 'BRANCH_PHONE')


#transforming the specified columns, from the mapping document, of the extracted credit df into new df
#just need to convert/concat the day, month, year columns into a TIMEID (YYYYMMDD)
#is there a way to use a timestamp and not just concat? is there an easier way?
tran_credit_spark = credit_spark.transform(tran_to_timeid, 'DAY', 'MONTH', 'YEAR')\
.drop('DAY','MONTH','YEAR')


#create and populate the requisite tables in SQL db creditcard_capstone, using overwrite to not muddle 
# up everthing by accidently appending or extending the table
tran_cust_spark.write.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
                                     user="root",
                                     password="password",
                                     url="jdbc:mysql://localhost:3306/creditcard_capstone",
                                     dbtable="CDW_SAPP_CUSTOMER ", 
                                     ).mode('overwrite').save()


tran_branch_spark.write.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
                                     user="root",
                                     password="password",
                                     url="jdbc:mysql://localhost:3306/creditcard_capstone",
                                     dbtable="CDW_SAPP_BRANCH", 
                                     ).mode('overwrite').save()


tran_credit_spark.write.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
                                     user="root",
                                     password="password",
                                     url="jdbc:mysql://localhost:3306/creditcard_capstone",
                                     dbtable="CDW_SAPP_CREDIT_CARD", 
                                     ).mode('overwrite').save()


# Read/load the data from the MySQL table
cap_spark = spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
                                             user="root",
                                             password="password",
                                             url="jdbc:mysql://localhost:3306/creditcard_capstone",
                                             dbtable="CDW_SAPP_BRANCH"
                                             ).load()


