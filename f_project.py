import pandas as pd
import pyspark as ps
from pyspark.sql import SparkSession
import matplotlib.pyplot as mp
from pyspark.sql.functions import *
import os
import re
from pyspark.sql.types import StringType
import numpy as np
import requests

# Creating Spark Session
spark = SparkSession.builder.appName('SBA 350').getOrCreate()
# Reading /loading the Dataset from JSON file, SPARK by defualt infersSchema for JSON files; only need headers parameter typically for csv files
customer_spark = spark.read.load("cdw_sapp_customer.json", format="json")
branch_spark = spark.read.load("cdw_sapp_branch.json", format= "json")
credit_spark = spark.read.load("cdw_sapp_credit.json", format= "json")

#transforms the passed df's column into title case(initcap)
def tran_cust_title_case(df, column_name):
    return df.withColumn(column_name,initcap(col(column_name)))
#transforms the passed df's column into lower case
def tran_cust_lower_case(df, column_name):
    return df.withColumn(column_name,lower(col(column_name)))
# concats the customer's street and apt numbers into single column 
def concat_cust_street_apt(df, col1, col2):
    #concat_ws concatenates multiple string columns 
    return df.withColumn('FULL_STREET_ADDRESS', concat_ws(',', col(col1), col(col2)))
#the cust_phone doesn't have an area code, need to rectify somehow
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
    return df.withColumn('BRANCH_ZIP', when(col('BRANCH_ZIP').isNull(), lit(99999)).otherwise(col('BRANCH_ZIP')))

#match cust state to branch state and slice branch phone 3-5 (last included?) to append to cust phone after 2nd element
#first left join the cust df and the branch df on their respective states like in sql
#getting duplicate rows are created for each row the join operation matches
custJoinbranch = customer_spark.join(branch_spark, customer_spark.CUST_STATE == branch_spark.BRANCH_STATE, 'left')

# Updating the customer phone by appending a sliced portion of the branch phone
customer_fix = custJoinbranch.withColumn('CUST_PHONE', concat(col('CUST_PHONE').substr(1, 2), col('BRANCH_PHONE').substr(3, 3), col('CUST_PHONE').substr(3, 7)))
unwanted_columns = branch_spark.columns
#asterisk unpacks the columns from branch_spark so drop doesn't receive it all as a single argument and understands they're
# individual column names. Drop duplicates on cust_ssn so different family members living together aren't dropped. W/o the column 
# selection we'd have 3700 distinct entries vs the 952 we started with
customer_fix = customer_fix.drop(*unwanted_columns).dropDuplicates(['SSN'])

# concat the day, month, year columns into a TIMEID (YYYYMMDD)
def tran_to_timeid(df, day, month, year):
    # first concat the columns so you can use the to_date function. Its format won't match the 
    # other tables. Need to lpad the month and day values so not to throw error when parsing too
    # to_date. The 2 represents the desired length of the string and the 0 is what we're left-padding 
    # with. Can't even do what the mapping logic wants YYYYMMDD
    date_string = concat(
        col(year),
        lpad(col(month), 2, '0'),
        lpad(col(day), 2, '0'))
    return df.withColumn('TIMEID', to_date(date_string, 'yyyyMMdd'))



#transforming the specified columns, from the mapping document, of the extracted customer df into new df
tran_cust_spark = customer_fix.transform(tran_cust_title_case, 'FIRST_NAME')\
.transform(tran_cust_lower_case, 'MIDDLE_NAME')\
.transform(tran_cust_title_case, 'LAST_NAME')\
.transform(concat_cust_street_apt, 'APT_NO', 'STREET_NAME')\
.drop('STREET_NAME', 'APT_NO')\
.transform(tran_phone_num, 'CUST_PHONE')


#transforming the specified columns, from the mapping document, of the extracted branch df into new df
#just need to transform the branch_zip if the source value is null load default (99999) value else direct move;
#also change branch_phone format to (xxx)xxx-xxxx
tran_branch_spark = branch_spark.transform(tran_branch_zip)\
.transform(tran_phone_num, 'BRANCH_PHONE')


#transforming the specified columns, from the mapping document, of the extracted credit df into new df
#just need to convert/concat the day, month, year columns into a TIMEID (YYYYMMDD)

###is there a way to use a timestamp and not just concat? is there an easier way?
tran_credit_spark = credit_spark.transform(tran_to_timeid, 'DAY', 'MONTH', 'YEAR')\
.drop('DAY','MONTH','YEAR')


#create and populate the requisite tables in SQL db creditcard_capstone, using overwrite to not muddle 
# up everthing by accidently appending or extending the table
tran_cust_spark.write.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
                                     user="root",
                                     password="password",
                                     url="jdbc:mysql://localhost:3306/creditcard_capstone",
                                     dbtable="CDW_SAPP_CUSTOMER", 
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


# Read the data from the MySQL table
cust_sql = spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
                                             user="root",
                                             password="password",
                                             url="jdbc:mysql://localhost:3306/creditcard_capstone",
                                             dbtable="CDW_SAPP_CUSTOMER"
                                             ).load()
branch_sql = spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
                                             user="root",
                                             password="password",
                                             url="jdbc:mysql://localhost:3306/creditcard_capstone",
                                             dbtable="CDW_SAPP_BRANCH"
                                             ).load()
credit_sql = spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
                                             user="root",
                                             password="password",
                                             url="jdbc:mysql://localhost:3306/creditcard_capstone",
                                             dbtable="CDW_SAPP_CREDIT_CARD"
                                             ).load()



#4. Functional Requirements - LOAN Application Dataset

#Create a Python program to GET (consume) data from the above API endpoint for the loan
#  application dataset.
bank_url = 'https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json'
response = requests.get(bank_url)
loan_data = response.json()

#Once Python reads data from the API, utilize PySpark to load data into RDBMS (SQL).
# The table name should be CDW-SAPP_loan_application in the database.
# Note: Use the “creditcard_capstone” database.
loan_df = spark.createDataFrame(loan_data)
loan_df.write.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
                                     user="root",
                                     password="password",
                                     url="jdbc:mysql://localhost:3306/creditcard_capstone",
                                     dbtable="CDW_SAPP_loan_application", 
                                     ).mode('overwrite').save()


### FUNCTIONS ###

# Clears the terminal screen, and displays a title bar.
def display_title_bar():
    os.system('cls')
    print("\t**********************************************")
    print("\t***  Welcome to Chad's Final Project!  ***")
    print("\t**********************************************")

#takes the user's input and then executes the corresponding functions
def get_user_choice():
    #1-3 are TRANSACTION DETAILS MODULE
    #next option to order by day and descending order(maybe make it optional for ascending as well?)
    print("[1] Display transactions by customers given zip code and date?.")
    print("[2] Display the number and total values of tranactions of a given TYPE.")
    print("[3] Display the number and total values of transactions for branches of a given STATE.")
    #4-7 are CUSTOMER DETAILS
    print("[4] Check account details of an existing customer.")
    print("[5] Modify the details of an existing customer's account.")
    print("[6] Generate a monthly bill for a credit card number, given the month and year.")
    print("[7] Display the transactions of a customer between two given dates.")
    #8-10 are plotting options
    print("[8] Plot which transaction type has a high rate of transactions.")
    print("[9] Plot which state has a high number of customers.")
    print("[10] Plot the sum of all transactions for the top 10 customers, and which customer"
          " has the highest transaction amount. ")
    print("[11] Plot the approved applications of self-employed applicants. ")
    print("[12] Plot the rejected applications of married men. ")
    print("[13] Plot the top three months with the highests amount of transactions. ")
    print("[14] Print response status code. ")
    
    print("[q] Quit.")
    
    return input("What would you like to do? ")

#1st choice
#Used to display the transactions made by customers living in a given zip code for a given month and
# year. Order by day in descending order.

#column date data, that isnt in the tran_credit_spark, is formatted in ISO 8601 standard 
#ie 2018-04-18T16:51:47.000-04:00 (YYYY-MM-DD T HH:MM:SS.SSS - TIMEZONE)
def disp_tran_by_cust_zip(zip, y, m):
    #join cust_sql with credit_sql on cust_ssn
    custJoinCredit =  cust_sql.join(credit_sql, cust_sql.SSN == credit_sql.CUST_SSN, 'left')
    #drop the ssn column and specifically the credit_sql.credit_card_no to avoid duplicates in the show object
    # could do the cust_spark one, depends on where you want the credit_card_no column to be on the table
    #just have to do .sort(desc('TIMEID')), will auomatically put it in desc by year, month, day. .sort() is the same as .orderBy() 
    custJoinCredit = custJoinCredit.drop('SSN', credit_sql.CREDIT_CARD_NO)
    custJoinCredit.filter((custJoinCredit.CUST_ZIP == zip) & 
                          (year(custJoinCredit.TIMEID) == y) &
                          (month(custJoinCredit.TIMEID) == m)).sort(desc('TIMEID')).show()

#2nd choice
# Used to display the number and total values of transactions for a given type. 
def disp_tran_total_by_type(t_type):
    # credit_sql.filter(credit_sql.TRANSACTION_TYPE == t_type).show()
    t_num = credit_sql.filter(credit_sql.TRANSACTION_TYPE == t_type).count()
        #collect the df transformation
        #need the list indeces at the end so it doesn't look all wonky; its a
        #single aggregated value so only need [0][0] with no incrementation required in loop
        #have to round because defulat is 6 decimals for w/e reason
    t_sum = credit_sql.filter(credit_sql.TRANSACTION_TYPE == t_type).select(round(sum(credit_sql.TRANSACTION_VALUE), 2)).collect()[0][0]
    print(f"The number of transactions for {t_type} is {t_num}. With their sum total being ${t_sum}\n")

#3rd choice
# Used to display the total number and total values of transactions for branches in a given state.    
def disp_tran_total_by_branch_state(state):
    #need to join branch_sql w/ credit_sql on branch_code; then filter by state and show the total
    # value and category of all transactions
    ttypes = ['Education', 'Entertainment', 'Healthcare', 'Grocery', 'Test', 'Gas', 'Bills']
    branchJoinCredit = branch_sql.join(credit_sql, branch_sql.BRANCH_CODE == credit_sql.BRANCH_CODE, 'left')
    #set it to a new variable for further filtering
    b = branchJoinCredit.filter(branchJoinCredit.BRANCH_STATE == state)
    for trans in ttypes:
        #filtering transactions column for only the current type from the ttypes list; use collect
        #to return the results as a list so we can set the
        #sum_value variable equal to it; need the list indeces at the end so it doesn't look all wonky;
        #its a single aggregated value so only need [0][0] with no incrementation required in loop
        #have to round because defualt is 6 decimals for w/e reason
        sum_value = b.filter(b.TRANSACTION_TYPE == trans).select(round(sum(b.TRANSACTION_VALUE), 2))\
            .collect()[0][0]
        t_count = b.filter(b.TRANSACTION_TYPE == trans).select(b.TRANSACTION_VALUE).count()
        print(f"There where {t_count} {trans} transactions in {state} with their value equating to: ${sum_value}")
    total_trans_value = b.select(round(sum(b.TRANSACTION_VALUE), 2)).collect()[0][0]
    trans_count = b.select(b.TRANSACTION_VALUE).count()
    print(f'The total amount of transactions in {state} is {trans_count}, with their sum being ${total_trans_value}')
    print()

#4th choice
# Used to check the existing account details of a customer.
def check_cust_exist(cust_ssn):
    #simple show from cust_sql
    cust_sql.filter(cust_sql.SSN == cust_ssn).show()

#5th choice
#  Used to modify the existing account details of a customer.
def modify_cust_account(cust_ssn, cust_sql):
    #have to do cust_sql.columns instead of just cust_sql so when using column in the loops print f
    #it just prints its name instead of all the extra wonky stuff
    temp_sql = cust_sql
    for column in cust_sql.columns:
        user_input = input(f"Do you want to modify customer's {column} (y/n): ")
        if user_input.lower() == 'y':
            new_value = input(f'Enter the new value for {column}: ')
            temp_sql = temp_sql.withColumn(column, when(temp_sql.SSN == cust_ssn, new_value).otherwise(col(column)))
        else:
            continue
    modified_row =temp_sql.filter(temp_sql.SSN == cust_ssn)
    modified_row.show()
    # spark.catalog.uncacheTable("CDW_SAPP_CUSTOMER")
    # spark.sql("DROP TABLE IF EXISTS CDW_SAPP_CUSTOMER")
    # Write the modified DataFrame back to the SQL database
    temp_sql.write.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
                                     user="root",
                                     password="password",
                                     url="jdbc:mysql://localhost:3306/creditcard_capstone",
                                     dbtable="CDW_SAPP_CUSTOMER_MODIFIED", 
                                     mode='overwrite').save()
    ### why does ).mode('overwrite').save()   overwrite the whole table as blank, when mode='overwrtie').save() throws ErrorIfExists?

#6th choice
# Used to generate a monthly bill for a credit card number for a given month and year. 
#column date data, that isnt in the tran_credit_spark, is formatted in ISO 8601 standard 
#ie 2018-04-18T16:51:47.000-04:00 (YYYY-MM-DD T HH:MM:SS.SSS - TIMEZONE)
# def gen_monthly_bill_by_card_number(card, year):
def gen_monthly_bill_by_card_number(card, y, m):
    # just have to access the credit_sql
    credit_sql.filter((year(credit_sql.TIMEID) == y ) & 
                      (month(credit_sql.TIMEID) == m ) & 
                      (credit_sql.CREDIT_CARD_NO == card)).sort(desc('TIMEID')).show()
    #have to round the sum because for w/e reason w/o it it somehow makes it 6 decimals
    # gave alias for legibility 
    credit_sql.filter((year(credit_sql.TIMEID) == y ) & 
                      (month(credit_sql.TIMEID) == m ) & 
                      (credit_sql.CREDIT_CARD_NO == card))\
    .select(round(sum(credit_sql.TRANSACTION_VALUE), 2).alias('Total Monthly Bill')).show()
   
#7th choice
# Used to display the transactions made by a customer between two dates.
# Order by year, month, and day in descending order.
# just using the credit_sql again. Going to use range, which is between in pyspark. So we're filtering
# by cust_ssn then betweening by sd - ed; just have to do .sort(desc('TIMEID')), will auomatically
# put it in desc by year, month, day. 
def disp_tran_by_cust_between_given_dates(cust_ssn, sd, ed):
    credit_sql.filter((credit_sql.CUST_SSN == cust_ssn) & credit_sql.TIMEID.between(sd,ed))\
    .sort(desc('TIMEID')).show()

#8th choice
# Find and plot which transaction type has a high rate of transactions. 
# just need to access the credit_sql to make visualization; had to import .pyplot specifically
#dunno why importing all of matplotlib doesn't work
def plot_high_tran_type():
    # have to use toPandas to be able to plot; use groupby to group the transaction types together
    panda_credit = credit_sql.toPandas()
    # then run the count of transactions of ea type; then run the sum of their values; 
    # adding the .max() and removing the sort_values does print the highes sum which is 351405.28 but
    # not its name which is Bills; have to do .size().max() to get just the numerical value of the highest transaction type. 
    p_sum = panda_credit.groupby('TRANSACTION_TYPE').TRANSACTION_VALUE.sum()
    p_count = panda_credit.groupby('TRANSACTION_TYPE').size()
    p_highest = panda_credit.groupby('TRANSACTION_TYPE').size().max()
    #have to set the idmax to a variable, can't do it in same line above or call it as panda_sum.idmax() w/o
    # removing the .max() from it since that makes it a single value and there'd be no idmax that way
    p_id = p_sum.idxmax()
    p_max = p_sum.max()
    print(f'The transaction type with the highest count is {p_id}, with a total count of {p_highest} summing up to ${p_max}')
    mp.plot(p_count)
    mp.xlabel('Transaction Types')
    mp.ylabel('Total count of each Transaction')
    mp.title('Total count of transactions by type')
    mp.xticks(rotation=45)
    mp.show()

# 9th choice
#  Find and plot which state has a high number of customers.
def plot_high_cust_state():
    #just need to use cust_spark
    panda_cust = cust_sql.toPandas()
    
    #x axis should be states and y axis should be number of customers who reside there
    #select the values counts for the cust_state column
    p_graph = panda_cust.CUST_STATE.value_counts()
    mp.figure(figsize=(12,6))
    mp.bar(p_graph.index, p_graph.values)
    mp.xlabel('States')
    mp.ylabel('Number of Customers')
    mp.title('Total count of Customers by State')
    mp.show()

# 10th choice
# plot the sum of all transactions for the top 10 customers, and which customer has the
#  highest transaction amount(amount as in sum? or who has the most transactions at all?)
def plot_sum_of_top_ten_cust():
    #need to use credit_spark, and cust_spark if want to match ssn with actual names
    panda_credit = credit_sql.toPandas()
    # panda_cust = cust_sql.toPandas()
    
    #x axis should be states and y axis should be number of customers who reside there
    #grouping by cust_ssn and then taking the sum of their transactions
    p_top = panda_credit.groupby('CUST_SSN').TRANSACTION_VALUE.sum()
    #sort customers in descending order by transaction sum
    p_top = p_top.sort_values(ascending=False)
    p_10 = p_top.head(10)
    mp.figure(figsize=(10,6))
    # mp.bar(p_10.index, p_10.values)
    p_10.plot(kind='bar')
    mp.xlabel('Customers')
    mp.ylabel('Sum of Transactions')
    mp.title('Sum of Top Ten Customers\' Transactions')
    mp.xticks(rotation=45)
    #had to adjust subplot to see the custoemrs ssn
    mp.subplots_adjust(bottom=0.2)
    mp.show()
    
#11th choice
def plot_self_employed_approved_loans():
    # 5. Functional Requirements - Data Analysis and Visualization for LOAN Application
    # After the data is loaded into the database, the business analyst team wants to analyze and
    #  visualize the data.
    # Use Python libraries for the below requirements:

    #Find and plot the percentage of applications approved for self-employed applicants.
    # Note: Take a screenshot of the graph. 

    #first make a variable counting the number of approved self-employed applicants
    approved_apps = loan_df.filter((loan_df.Self_Employed == 'Yes') & 
                        (loan_df.Application_Status == 'Y' )).count()
    #then another variable counting the total amount of self-employed applications
    total_apps = loan_df.filter((loan_df.Self_Employed == 'Yes')).count()
    #calculate the percentage of approve_apps vs total_apps
    # app_percent = (approved_apps / total_apps) * 100
    non_approved_apps = total_apps - approved_apps
    labels = ['Approved Applications', 'Non-Approved Applications']
    slices = [approved_apps, non_approved_apps]
    mp.title('Approved Applications for Self-Employed Applicants')
    explode = [0.2, 0]
    mp.pie(slices, labels=labels, autopct='%.2f%%', startangle=90, explode=explode)                      
    mp.show()    

#12th choice    
def plot_rejected_male_married_apps():
    #Find the percentage of rejection for married male applicants.
    # Note: Take a screenshot of the graph.
    rejected_married_male_apps = loan_df.filter((loan_df.Married == 'Yes') & (loan_df.Gender == 'Male') & (loan_df.Application_Status == 'N' )).count()
    total_married_male_apps = loan_df.filter((loan_df.Married == 'Yes') & (loan_df.Gender == 'Male')).count()
    approved_married_male_apps = total_married_male_apps - rejected_married_male_apps
    # male_bar = pd.Series([rejected_married_male_apps, approved_married_male_apps, total_married_male_apps], 
    #                      index=['Rejected Applications', 'Approved Applications', 'Total Applications'])
    labels = ['Rejected Applications', 'Approved Applications']
    slices = [rejected_married_male_apps, approved_married_male_apps]
    mp.title('Rejected Applications of Married Men')
    explode = [0.2, 0] 
    mp.pie(slices, labels=labels, autopct='%.2f%%', startangle=90, explode=explode)                
    # mp.xticks(rotation=90)
    # mp.xticks(male_bar.index, ['Rejected Applications', 'Approved Applications', 'Total Applications'], rotation=90)
    mp.show()
    
#13th choice
def plot_top_three_sale_months():
    #Find and plot the top three months with the largest transaction data.
    # Note: Take a screenshot of the graph.
    #need to just use credit_sql; switch .size() and .sum() depending on what they wanted. 
    panda_credit_temp = credit_sql.toPandas()
    top_three = panda_credit_temp.groupby(pd.DatetimeIndex(panda_credit_temp.TIMEID).month)\
        .TRANSACTION_VALUE.size()
    # #sort months in descending order by transaction count
    top_three = top_three.sort_values(ascending=False)
    p_3 = top_three.head(3)
    # mp.figure(figsize=(10,6))
    p_3.plot(kind='bar', color='magenta')
    for i, value in enumerate(p_3):
        mp.text(i, value, str(value), ha='center', va='bottom')
    # mp.xlabel('Months')
    mp.ylabel('Count of Transactions')
    mp.title('Total number of Transactions for the Top Three Months')
    # mp.ylim(0, top=top_three.max() + 100)
    # mp.yticks(range(0, top_three.max() + 200, 100))
    mp.xticks(rotation=45)
    mp.show()
    
#14th choice
def print_response_status_code():
    #Find the status code of the above API endpoint.
    # Hint: status code could be 200, 400, 404, 401.
    print(response.status_code)
    print() 
    
#check case function
#can you make it one loop in a single function to check every case passed to it and not just type 
#specific (ie need different function for date, zip)
zip_pattern = r'\d{5}'
month_pattern = r'(0[1-9]|1[0-2])'
year_pattern = r'\d{4}$'
#in this specific version will only be able to accept year ranges from 1900-2099. For scalability would need to change or
# revert it to the original \d{4} 
date_pattern = r'(19\d{2}|20\d{2})\-(0[1-9]|1[0-2])\-(0[1-9]|[1-2][0-9])'
ssn_pattern = r'^\d{9}$'
# compile to be able to use re.IGNORECASE or (?i) to make case insensitive
transaction_type_pattern = r'(?i)(education|entertainment|healthcare|grocery|test|gas|bills)'
credit_card_pattern = r'\d{16}'
state_pattern = r'[A-Za-z]{2}'

#will this force the user to enter in the correct input?
def check_case(user_input, pattern):
    #have to use re.match(user_input, pattern) instead of user_input.match(pattern) because it needs to be a regular expression object
    #have to pass the pattern as the frist object to match vs the user_input
    #had to change from .match to .fullmatch because when validating the user input for the month if would accept 4 digit
    #strings as long as part of it matched one of the check cases, even when using ^ and $
    while not re.fullmatch(pattern, user_input):
            user_input = input("Please enter a correctly fomated value: ")
    return user_input


### MAIN PROGRAM ###

# Set up a loop where users can choose what they'd like to do.
choice = ''
display_title_bar()
while choice != 'q':    

    #shows menu options then returns the user's choice
    choice = get_user_choice()
     
    # if ele switch statement. Prompts user input given by their menu choice, then checks their input vs correct input pattern before calling the appropriate function.
    if choice == '1':
        zip = input("Enter the 5 digit desired zipcode: ")
        #have to set the variables to the function call so it stores the return value to be used after second check case. Was having issue, after purposefully misinputing 
        # the first check case, if then inputing the second check case correctly it displayed the queried information. But if misinputing the second check case, then the correct syntax, if would just
        # display the column names. 
        zip = check_case(zip, zip_pattern)
        y = input("Enter in year (yyyy): ")
        y = check_case(y, year_pattern)
        m = input("Enter in month (MM): ")
        m = check_case(m, month_pattern)
        disp_tran_by_cust_zip(zip, y, m)
    elif choice == '2':
        t_type = input("Enter desired transaction type(Education, Entertainment, Healthcare, Grocery, Test, Gas, Bills): ")
        t_type = check_case(t_type, transaction_type_pattern)
        disp_tran_total_by_type(t_type)
    elif choice == '3':
        state = input("Enter desired state of query: ")
        state = check_case(state, state_pattern)
        disp_tran_total_by_branch_state(state)
    elif choice == '4':
        cust_ssn = input("Enter in customer's SSN: ")
        cust_ssn = check_case(cust_ssn, ssn_pattern)
        check_cust_exist(cust_ssn)
    elif choice == '5':
        cust_ssn = input("Enter customer's SSN for the acconut account you wish to modify: ")
        cust_ssn = check_case(cust_ssn, ssn_pattern)
        modify_cust_account(cust_ssn, cust_sql)
    elif choice == '6':
        card = input("Enter in credit card number: ")
        card = check_case(card, credit_card_pattern)
        y = input("Enter in year (yyyy): ")
        y = check_case(y, year_pattern)
        m = input("Enter in month (MM): ")
        m = check_case(m, month_pattern)
        gen_monthly_bill_by_card_number(card, y, m)
    elif choice == '7':
        cust_ssn = input("Enter in customer's SSN number: ")
        cust_ssn = check_case(cust_ssn, ssn_pattern)
        sd = input("Enter in start date (YYYY-MM-DD): ")
        sd = check_case(sd, date_pattern)
        ed = input("Enter in end date (YYYY-MM-DD): ")
        ed = check_case(ed, date_pattern)
        disp_tran_by_cust_between_given_dates(cust_ssn, sd, ed)
    elif choice == '8':
        plot_high_tran_type()
    elif choice == '9':
        plot_high_cust_state()
    elif choice == '10':
        plot_sum_of_top_ten_cust()
    elif choice == '11':
        plot_self_employed_approved_loans()
    elif choice == '12':
        plot_rejected_male_married_apps()
    elif choice == '13':
        plot_top_three_sale_months()
    elif choice == '14':
        print_response_status_code()
    elif choice == 'q':
        print("\nExiting program.")
    else:
        print("\nI didn't understand that choice.\n")