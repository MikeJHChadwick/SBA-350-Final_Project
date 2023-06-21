import pandas as pd
import pyspark as ps
from pyspark.sql import SparkSession
import matplotlib as mp
from pyspark.sql.functions import *
import os

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



# print("Welcome to Mike Chadwick's Final Project 350!")
# #from time import sleep
# sleep(3)
# #way to make the program understand if its windows or linux? to make it crossplatform compatible?
# os.system('cls') #'clear' on linux machines


# Greeter is a terminal application that greets old friends warmly,
#   and remembers new friends.


### FUNCTIONS ###

def display_title_bar():
    # Clears the terminal screen, and displays a title bar.
    os.system('cls')
    #prints terminal title header          
    print("\t**********************************************")
    print("\t***  Greeter - Hello old and new friends!  ***")
    print("\t**********************************************")

def get_user_choice(choice):
    #1-3 are TRANSACTION DETAILS MODULE
    #next option to order by day and descending order(maybe make it optional for ascending as well?)
    print("[1] Display transactions by customers given zip code and date?.")
    
    #next level give them the choice of options education, entertainment, healthcare, grocery, test,
    #gas, bills
    print("[2] Display the number and total values of tranactions of a given TYPE.")
    print("[3] Display the number and total values of transactions for branches of a given STATE.")
    
    
    #4-7 are CUSTOMER DETAILS
    print("[4] Check account details of an existing customer.")
    print("[5] Modify the details of an existing customer's account.")
    print("[6] Generate a monthly bill for a credit card number, given the month and year.")
    #order by year, month, day in descending order(add ascending?)
    print("[7] Display the transactions of a customer between two given dates.")
    
    
    # print("[8] Plot which transaction type has a high rate of transactions.")
    # print("[9] Plot which state has a high number of customers.")
    # print("[10] Plotthe sum of all transactions for the top 10 customers, and which customer 
    # has the higherst transaction amount. HINT USE CUST_SSN.")
    
    print("[q] Quit.")
    
    return input("What would you like to do? ")

#1st choice
def disp_tran_by_cust_zip(zip, date):
    #check zip leng/structure(5 digits)
    #check date length/structure etc
    print()

#2nd choice
def disp_tran_total_by_type(t_type):
    print()

#3rd choice    
def disp_tran_total_by_branch_state(state):
    print()

#4th choice
def check_cust_exist(cust_ssn):
    print()

#5th choice 
def modify_cust_account(cust_ssn):
    #check cust numb(cust_ssn) length(9 digits) etc
    print()

#6th choice
def gen_monthly_bill_by_card_number(card, start_date, end_date):
    print()

#7th choice
def disp_tran_by_cust_between_given_dates():
    print()

#8th choice
# def disp_tran_by_cust_zip():
#     print()

#check case function
#can you make a loop to keep prompting the user until the desired input is met?
#can you make it one loop in a single function to check every case passed to it and not just type 
#specific (ie need different function for date, zip)
zip_pattern = r'\d{5}$'
date_pattern = r''
ssn_pattern = r'^\d{9}$'
file_type_pattern = r''
credit_card_pattern = r'^\d{16}$'
#does it match with table even if user inputs only lower case ?
state_pattern = r'^[A-Za-z]{2}$'

def check_case(user_input, pattern):
    # return len(zip) == 5 and zip.isdigit()
    while not user_input.match(pattern):
            user_input = input("Please enter a correctly fomated value: ")
    return user_input

### MAIN PROGRAM ###

# Set up a loop where users can choose what they'd like to do.
choice = ''
display_title_bar()
while choice != 'q':    

    #shows menu options then returns the user's choice
    choice = get_user_choice()
     
    # Respond to the user's choice.
    if choice == '1':
        zip = input("Enter the 5 digit desired zipcode: ")
        check_case(zip, zip_pattern)
        date = input("Enter desired date (YYYY-MM-DD): ")
        check_case(date, date_pattern)
        disp_tran_by_cust_zip(zip, date)
    elif choice == '2':
        t_type = input("Enter desired file type: ")
        check_case(t_type, file_type_pattern)
        disp_tran_total_by_type(t_type)
    elif choice == '3':
        state = input("Enter desired state of query: ")
        check_case(state, state_pattern)
        disp_tran_total_by_branch_state(state)
    elif choice == '4':
        cust_ssn = input("Enter in customer's SSN: ")
        check_case(cust_ssn, ssn_pattern)
        check_cust_exist(cust_ssn, ssn_pattern)
    elif choice == '5':
        cust_ssn = input("Enter customer's SSN for the acconut account you wish to modify: ")
        check_case(cust_ssn, ssn_pattern)
        modify_cust_account(cust_ssn)
    elif choice == '6':
        card = input("Enter in credit card number: ")
        check_case(card, credit_card_pattern)
        date = input("Enter in start date (YYYY-MM-DD): ")
        check_case(date, date_pattern)
        date2 = input("Enter in end date (YYYY-MM-DD): ")
        check_case(date2, date_pattern)
        gen_monthly_bill_by_card_number(card, date, date2)
    elif choice == '7':
        cust_ssn = input("Enter in customer's SSN number: ")
        check_case(cust_ssn, ssn_pattern)
        date = input("Enter in start date (YYYY-MM-DD): ")
        check_case(date, date_pattern)
        date2 = input("Enter in end date (YYYY-MM-DD): ")
        check_case(date2, date_pattern)
        disp_tran_by_cust_between_given_dates(cust_ssn, date, date2)
    # elif choice == '8':
    #     def disp_tran_by_cust_zip()
    # elif choice == '9':
        # print("\nI can't wait to meet this person!\n")
    elif choice == 'q':
        print("\nExiting program.")
    else:
        print("\nI didn't understand that choice.\n")