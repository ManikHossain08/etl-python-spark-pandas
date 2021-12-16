from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql import SQLContext

'''
INTRODUCTION

The goal of this coding challenge is to build ETL data pipeline that read some text files and transform those
data according to the business requirements and load into directory in csv format.
'''

'''
DATASET

There are two csv data sets located on the 'data' directory of this project and output will be store on
'output directory'
'''

# Initialize a spark context.
sc = SparkContext('local', 'Rivet ETL Challenge')
spark = SQLContext(sc)

'''
EXTRACT DATA FROM SOURCE

This section will read data from 'data' directory and transform them, then register view from data frame
We register two views one for invoice and another for payments from spark data frame after 
reading from local directory
'''
invoicesDF = spark.read.format("csv").option("header", "true").load("data/invoices.csv") \
    .withColumn("invDate", to_date(col("invoice_date"), "yyyy-MM-dd")) \
    .drop("invoice_date").withColumnRenamed("invDate", "invoice_date")
invoicesDF.createOrReplaceTempView("invoices")

paymentsDF = spark.read.format("csv").option("header", "true").load("data/payments.csv") \
    .withColumn("payDate", to_date(col("payment_date"), "yyyy-MM-dd")) \
    .drop("payment_date").withColumnRenamed("payDate", "payment_date")
paymentsDF.createOrReplaceTempView("payments")

'''
PERCENTILE CALCULATION

In this section I calculate the percentile of each customer based on the invoice amount and store it 
in data frame then register a view to use later for processed customer.
'''
percentileDF = invoicesDF.groupby('customer_id').agg(
    expr('percentile(invoice_amount, array(0.90))')[0].alias('pct_90'),
    expr('percentile(invoice_amount, array(0.70))')[0].alias('pct_70'))
percentileDF.createOrReplaceTempView("percentileTbl")

# 1. processed-customers.csv generation ######
'''
PROCESSED CUSTOMER GENERATION

In this section I aggregate the invoices to achieve the specific result into multiple query
'''
# ## this query group customer and get all the aggregated features from invoice toward the goal.
spark.sql("SELECT i.customer_id, i.customer_name, sum(i.invoice_amount) total_invoiced_amount, \
          count(*) total_invoice_count, sum(i.invoice_balance) unpaid_amount, \
          count( (CASE WHEN i.invoice_balance > 0.0 then 1 ELSE null END) ) unpaid_count,\
          min(i.invoice_date) first_invoice_date\
          FROM invoices i group by i.customer_id, i.customer_name").createOrReplaceTempView("aggregatedCustomers")

# ## this query get the 'first invoice amount' based on min(invoice_date) that found in the previous query and joining
# with invoices table to get first_invoice_amount from there
spark.sql("select b.*, i.invoice_amount first_invoice_amount from aggregatedCustomers b \
          left join invoices i \
          on b.first_invoice_date =  i.invoice_date and b.customer_id = i.customer_id") \
    .createOrReplaceTempView("aggregatedCustomerTbl")

# ## this query get all the information of latest payment from the payments table from grouped customer with
# max(payment_date) by joining with same table.
# Here I use CTE to get latest payment from each customer and then join with same table to get latest payment of
# that day and register another view.
spark.sql("with base as (select customer_id, max(payment_date) payment_date from payments group by customer_id)\
          select b.*, p.payment_amount from base b \
          inner join payments p \
          on b.payment_date =  p.payment_date and  b.customer_id = p.customer_id") \
    .createOrReplaceTempView("latestPaymentInfo")

# ## this query calculate the frequency of customer in terms of HIGH, MEDIUM, LOW after joining with the percentile
# table as customer_segment. Here through this query we will get our desire output called processed-customer.csv
# this result of processed customer will be used for generating the result of 'processed-invoice.csv'
# and 'processed-payments.csv'
processedCustomerDF = spark.sql("select pc.*, p.payment_date last_payment_date, p.payment_amount last_payment_amount,\
                        CASE WHEN pc.total_invoiced_amount >= pt.pct_90 THEN 'High' \
                             WHEN pc.total_invoiced_amount > pt.pct_70 and pc.total_invoiced_amount < pt.pct_90 THEN 'Medium' \
                             ELSE 'Low' \
                        END AS customer_segment\
                        from aggregatedCustomerTbl pc \
                        left join latestPaymentInfo p on pc.customer_id = p.customer_id\
                        left join percentileTbl pt on pc.customer_id = pt.customer_id")
processedCustomerDF.createOrReplaceTempView("processedCustomersTbl")

# 2. processed-invoices.csv generation #####
'''
PROCESSED INVOICE GENERATION

In this section, I used invoice table along with processedCustomersTbl to get processed invoice 
'''
# ### this query will calculate the 'invoice due date' according to the payment_terms and result will be keep in
# the populatedInvoice view to reuse later process.
spark.sql("select *, \
          CASE WHEN payment_terms = 'Net 30' THEN date_add(invoice_date, 30) \
               WHEN payment_terms = 'Net 45' THEN date_add(invoice_date, 45) \
               WHEN payment_terms = 'Net 60' THEN date_add(invoice_date, 60) \
               ELSE invoice_date \
          END AS invoice_due_date \
          from invoices").createOrReplaceTempView("populatedInvoice")

# ### this query will join 'populatedInvoice' and 'processedCustomersTbl' to achieve the goal of this processed invoice
# here we also calculate 'invoice_overdue' from 'invoice_due_date' and current date.
processedInvoiceDF = spark.sql("select i.customer_id, i.customer_name, i.invoice_id, i.invoice_amount, i.invoice_balance,\
          i.invoice_date, i.payment_terms, i.invoice_due_date,\
          CASE WHEN current_date() > i.invoice_due_date  THEN true ELSE false \
          END AS invoice_overdue, pc.customer_segment\
          from populatedInvoice i left join processedCustomersTbl pc on i.customer_id = pc.customer_id")

# 3. processed-payments.csv generation ####
'''
PROCESSED PAYMENTS GENERATION

In this section, I used payments table along with processedCustomersTbl to get processed payments 
'''
# ### this query simply do left join 'payments' with 'processedCustomersTbl'
processedPaymentDF = spark.sql("select p.customer_id, pc.customer_name, p.payment_id, p.payment_amount, p.payment_date, p.invoice_id, pc.customer_segment \
          from payments p left join processedCustomersTbl pc on p.customer_id = pc.customer_id")

# export all the processed data into csv file format using pandas.
processedCustomerDF.toPandas().to_csv('output/processed-customers.csv', index=False)
processedInvoiceDF.toPandas().to_csv('output/processed-invoices.csv', index=False)
processedPaymentDF.toPandas().to_csv('output/processed-payments.csv', index=False)

'''
FUNCTION IMPLEMENTATION FOR TEST

This section implements the function for evaluating the test cases of all the resultant data set. All the 
test codes are appeared on the 'test' folder and all other associated files. 
'''


def count_processed_payment():
    """
    This function will count the number of processed payments and return the count for testing purpose
    """
    return processedPaymentDF.count()


def get_processed_payment_df():
    """
    this function will read the dataframe and create a text string for each row separated by new line '\n'
    to match with resultant data for testing.
    """

    process_payments = spark.sql("select * from processedPaymentTbl")

    text = ""
    for i in range(1, process_payments.count()):
        text += process_payments.collect()[i][0] + "," + process_payments.collect()[i][1] + "," + \
                process_payments.collect()[i][2] \
                + "," + process_payments.collect()[i][3] + ", " + process_payments.collect()[i][4] \
                + "," + process_payments.collect()[i][5] + "," + process_payments.collect()[i][6] + "\n"

    return text


def count_processed_invoices():
    """
    This function will count the number of processed invoices and return the count for testing purpose
    """
    return processedInvoiceDF.count()


def get_processed_invoices_df():
    """
    this function will read the dataframe and create a text string for each row separated by new line '\n'
    to match with resultant data for testing.
    """

    process_invoices = spark.sql("select * from processedInvoiceTbl")

    text = ""
    for i in range(1, process_invoices.count()):
        text += process_invoices.collect()[i][0] + "," + process_invoices.collect()[i][1] + "," + \
                process_invoices.collect()[i][2] \
                + "," + process_invoices.collect()[i][3] + ", " + process_invoices.collect()[i][4] \
                + "," + process_invoices.collect()[i][5] + "," + process_invoices.collect()[i][6] \
                + "," + process_invoices.collect()[i][7] + ", " + process_invoices.collect()[i][8] \
                + "," + process_invoices.collect()[i][9] + "\n"
    return text


def count_processed_customers():
    """
    This function will count the number of processed invoices and return the count for testing purpose
    """
    return processedCustomerDF.count()


def get_processed_customers_df():
    """
    this function will read the dataframe and create a text string for each row separated by new line '\n'
    to match with resultant data for testing.
    """

    process_customers = spark.sql("select * from processedCustomersTbl")

    text = ""
    for i in range(1, process_customers.count()):
        text += process_customers.collect()[i][0] + "," + process_customers.collect()[i][1] + "," + \
                process_customers.collect()[i][2] \
                + "," + process_customers.collect()[i][3] + ", " + process_customers.collect()[i][4] \
                + "," + process_customers.collect()[i][5] + "," + process_customers.collect()[i][6] \
                + "," + process_customers.collect()[i][7] + ", " + process_customers.collect()[i][8] \
                + "," + process_customers.collect()[i][9] + ", " + process_customers.collect()[i][10] + "\n"

    return text
