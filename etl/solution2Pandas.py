from sqlite3 import connect
import pandas as pd
import numpy as np

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

# Initialize a connection for sqlite3 to supprt pandas.
conn = connect(':memory:')

'''
EXTRACT DATA FROM SOURCE

This section will read data from 'data' directory and transform them, then register view from data frame
We register two views one for invoice and another for payments from spark data frame after 
reading from local directory
'''

invoicesDF = pd.read_csv("data/invoices.csv")
invoicesDF['invoice_date'] = pd.to_datetime(invoicesDF['invoice_date'], format="%Y-%m-%d")

paymentsDF = pd.read_csv("data/payments.csv")
paymentsDF['payment_date'] = pd.to_datetime(paymentsDF['payment_date'], format="%Y-%m-%d")

invoicesDF.to_sql('invoices', conn)
paymentsDF.to_sql('payments', conn)

'''
PERCENTILE CALCULATION

In this section I calculate the percentile of each customer based on the invoice amount and store it 
in data frame then register a view to use later for processed customer.
'''


def percentile(n):
    def percentile_(x):
        return np.percentile(x, n)

    percentile_.__name__ = 'percentile_%s' % n
    return percentile_


percentileDF = invoicesDF.groupby('customer_id').agg({'invoice_amount': [percentile(90), percentile(70)]})
percentileDF.to_sql('invoicePercentile', conn)
percentileDF = pd.read_sql('SELECT * FROM invoicePercentile', conn)
percentileDF.rename(columns={list(percentileDF)[1]: 'pct_90', list(percentileDF)[2]: 'pct_70'}, inplace=True)
percentileDF.to_sql('percentileTbl', conn)

# 1. processed-customers.csv generation ######
'''
PROCESSED CUSTOMER GENERATION

In this section I aggregate the invoices to achieve the specific result into multiple query
'''
# ## this query group customer and get all the aggregated features from invoice toward the goal.
aggregatedCustomers = pd.read_sql("SELECT i.customer_id, i.customer_name, sum(i.invoice_amount) total_invoiced_amount, \
          count(*) total_invoice_count, sum(i.invoice_balance) unpaid_amount, \
          count( (CASE WHEN i.invoice_balance > 0.0 then 1 ELSE null END) ) unpaid_count,\
          min(i.invoice_date) first_invoice_date\
          FROM invoices i group by i.customer_id, i.customer_name", conn)
aggregatedCustomers.to_sql('aggregatedCustomersTbl', conn)

# ## this query get the 'first invoice amount' based on min(invoice_date) that found in the previous query and joining
# with invoices table to get first_invoice_amount from there
aggregatedDF = pd.read_sql("select b.*, i.invoice_amount from aggregatedCustomersTbl b \
                          left join invoices i \
                          on b.first_invoice_date =  i.invoice_date and b.customer_id = i.customer_id", conn)
aggregatedDF.to_sql('aggregatedTbl', conn, if_exists='replace')

# ## this query get all the information of latest payment from the payments table from grouped customer with
# max(payment_date) by joining with same table.
# Here I use CTE to get latest payment from each customer and then join with same table to get latest payment of
# that day and register another view.
latestPaymentInfoDF = pd.read_sql("with base as (select customer_id, max(payment_date) payment_date from payments group by customer_id)\
                              select b.*, p.payment_amount from base b \
                              inner join payments p \
                              on b.payment_date =  p.payment_date and  b.customer_id = p.customer_id", conn)
latestPaymentInfoDF.to_sql('latestPaymentInfoTbl', conn, if_exists='replace')

# ## this query calculate the frequency of customer in terms of HIGH, MEDIUM, LOW after joining with the percentile
# table as customer_segment. Here through this query we will get our desire output called processed-customer.csv
# this result of processed customer will be used for generating the result of 'processed-invoice.csv'
# and 'processed-payments.csv'
processedCustomerDF = pd.read_sql("select pc.customer_id,pc.customer_name, pc.total_invoiced_amount,\
                                   pc.total_invoice_count, pc.unpaid_amount, pc.unpaid_count, pc.first_invoice_date, pc.invoice_amount,\
                                   p.payment_date last_payment_date, p.payment_amount last_payment_amount,\
                                   CASE WHEN pc.total_invoiced_amount >= pt.pct_90 THEN 'High' \
                                     WHEN pc.total_invoiced_amount > pt.pct_70 and pc.total_invoiced_amount < pt.pct_90 THEN 'Medium' \
                                     ELSE 'Low' \
                                   END AS customer_segment\
                                from aggregatedTbl pc \
                                left join latestPaymentInfoTbl p on pc.customer_id = p.customer_id\
                                left join percentileTbl pt on pc.customer_id = pt.customer_id", conn)
processedCustomerDF.to_sql('processedCustomersTbl', conn, if_exists='replace')

# 2. processed-invoices.csv generation #####
'''
PROCESSED INVOICE GENERATION

In this section, I used invoice table along with processedCustomersTbl to get processed invoice 
'''
# ### this query will calculate the 'invoice due date' according to the payment_terms and result will be keep in
# the populatedInvoice view to reuse later process.
populatedInvoiceDF = pd.read_sql("select *, \
          CASE WHEN payment_terms = 'Net 30' THEN DATE(invoice_date, '30 day') \
               WHEN payment_terms = 'Net 45' THEN DATE(invoice_date, '45 day') \
               WHEN payment_terms = 'Net 60' THEN DATE(invoice_date, '60 day') \
               ELSE invoice_date \
          END AS invoice_due_date \
          from invoices", conn)
populatedInvoiceDF.to_sql('populatedInvoiceTbl', conn, if_exists='replace')

# ### this query will join 'populatedInvoice' and 'processedCustomersTbl' to achieve the goal of this processed invoice
# here we also calculate 'invoice_overdue' from 'invoice_due_date' and current date.
processedInvoiceDF = pd.read_sql("select i.customer_id, i.customer_name, i.invoice_id, i.invoice_amount,i.invoice_balance, \
          i.invoice_date, i.payment_terms, i.invoice_due_date,\
          CASE WHEN date('now') > i.invoice_due_date  THEN 'true' ELSE 'false' \
          END AS invoice_overdue, pc.customer_segment\
          from populatedInvoiceTbl i left join processedCustomersTbl pc on i.customer_id = pc.customer_id", conn)

processedInvoiceDF.to_sql('processedInvoiceTbl', conn, if_exists='replace')

# 3. processed-payments.csv generation  ####
'''
PROCESSED PAYMENTS GENERATION

In this section, I used payments table along with processedCustomersTbl to get processed payments 
'''
# ### this query simply do left join 'payments' with 'processedCustomersTbl'
processedPaymentDF = pd.read_sql("select p.customer_id, pc.customer_name, p.payment_id, p.payment_amount, \
          p.payment_date, p.invoice_id, pc.customer_segment \
          from payments p left join processedCustomersTbl pc on p.customer_id = pc.customer_id", conn)
processedPaymentDF.to_sql('processedPaymentTbl', conn, if_exists='replace')

# export all the processed data into csv file format using pandas.
processedPaymentDF.to_csv('output/processed-payment1.csv', index=False)
processedCustomerDF.to_csv('output/processed-customer1.csv', index=False)
processedInvoiceDF.to_csv('output/processed-invoice1.csv', index=False)

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

    process_payments = pd.read_sql("select * from processedPaymentTbl", conn)

    text = ""
    for i in range(1, process_payments.count()):
        text += process_payments.collect()[i][0] + "," + process_payments.collect()[i][1] + "," + \
                process_payments.collect()[i][2] \
                + "," + process_payments.collect()[i][3] + "," + process_payments.collect()[i][4] \
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

    process_invoices = pd.read_sql("select * from processedInvoiceTbl", conn)

    text = ""
    for i in range(1, process_invoices.count()):
        text += process_invoices.collect()[i][0] + "," + process_invoices.collect()[i][1] + "," + \
                process_invoices.collect()[i][2] \
                + "," + process_invoices.collect()[i][3] + "," + process_invoices.collect()[i][4] \
                + "," + process_invoices.collect()[i][5] + "," + process_invoices.collect()[i][6] \
                + "," + process_invoices.collect()[i][7] + "," + process_invoices.collect()[i][8] \
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

    process_customers = pd.read_sql("select * from processedCustomersTbl", conn)

    text = ""
    for i in range(1, process_customers.count()):
        text += process_customers.collect()[i][0] + "," + process_customers.collect()[i][1] + "," + \
                process_customers.collect()[i][2] \
                + "," + process_customers.collect()[i][3] + "," + process_customers.collect()[i][4] \
                + "," + process_customers.collect()[i][5] + "," + process_customers.collect()[i][6] \
                + "," + process_customers.collect()[i][7] + "," + process_customers.collect()[i][8] \
                + "," + process_customers.collect()[i][9] + "," + process_customers.collect()[i][10] + "\n"
    return text
