# ETL Challenge #

## Instructions ##

Develop a solution to present the outputs listed from the input data files listed below.

Your solution will be evaluated based on the data structures and algorithms used, cleanliness of code (naming, organization, documentation, etc), overall design, and the tests developed.

Please submit your solution within two business days. Please reach out if you have any questions, we look forward to hearing from you!

## Input ##
Your input will be 2 csv files listed below - 

#### invoices.csv
| Columns Name   |      Description      |
|----------|-------------|
| customer_id |  Customer Id |
| customer_name |    Customer Name   |
| invoice_id | Invoice Id |
| invoice_amount | Invoice amount |
| invoice_balance | Amount yet to be paid for this invoice |
| invoice_date | Date of issue of the invoice |
| payment_terms | Payment terms of the invoice |

#### payments.csv
| Columns Name   |      Description      |
|----------|-------------|
| customer_id |  Customer Id |
| payment_id | Payment Id |
| payment_amount | Payment amount |
| payment_date | Date of the payment |
| invoice_id | Invoice against which the payment is made |


## Output ##
Your solution must output 3 files with the columns listed below.

#### processed-customers.csv

| Columns Name   |      Description      |
|----------|-------------|
| customer_id |  Customer Id |
| customer_name |    Customer Name   |
| total_invoiced_amount | Sum of all invoices for the customer |
| total_invoice_count | Total count of all invoices for the customer |
| unpaid_amount | Total amount that is yet to be paid by this customer |
| unpaid_count | Total number of unpaid invoices for this customer |
| first_invoice_date | Date of the first invoice for this customer |
| first_invoice_amount | Amount of the first invoice for this customer |
| last_payment_date | Date of the latest payment for this customer |
| last_payment_amount | Amount of the latest payment for this customer |
| customer_segment | 'High' if total_invoiced_amount > 90th percentile, 'Medium' if total_invoiced_amount between 70th and 90th percentile, 'Low' otherwise |

#### processed-invoices.csv
| Columns Name   |      Description      |
|----------|-------------|
| customer_id |  Customer Id |
| customer_name |    Customer Name   |
| invoice_id | Invoice Id |
| invoice_amount | Invoice amount |
| invoice_balance | Amount yet to be paid for this invoice |
| invoice_date | Date of issue of the invoice |
| payment_terms | Payment terms of the invoice |
| invoice_due_date | invoice_date + 30 days if payment_terms is 'Net 30', invoice_date + 45 days if payment_terms is 'Net 45', invoice_date + 60 days if payment_terms is 'Net 60' |
| invoice_overdue | true if today > invoice_due_date, false otherwise |
| customer_segment | Customer segment (maps to customer_segment from customers.csv |

#### processed-payments.csv
| Columns Name   |      Description      |
|----------|-------------|
| customer_id |  Customer Id |
| customer_name |    Customer Name   |
| payment_id | Payment Id |
| payment_amount | Payment amount |
| payment_date | Date of the payment |
| invoice_id | Invoice against which the payment is made |
| customer_segment | Customer segment (maps to customer_segment from customers.csv |