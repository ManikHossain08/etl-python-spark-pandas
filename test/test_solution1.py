from etl import solution1Spark


def test_count_processed_payments():
    count = solution1Spark.count_processed_payment()
    assert(count == 10)


def test_processed_payments_df():
    text_data = solution1Spark.get_processed_payment_df()
    try:
        out = open("tests/processed-payments","r").read()
        assert(text_data == out)
    except:
        out = open("tests/processed-payments","r", encoding="ISO-8859-1").read()
        assert(text_data == out)


def test_count_processed_invoices():
    count = solution1Spark.count_processed_invoices()
    assert(count == 10)


def test_processed_invoices_df():
    text_data = solution1Spark.get_processed_invoices_df()
    try:
        out = open("tests/processed-invoices","r").read()
        assert(text_data == out)
    except:
        out = open("tests/processed-invoices","r", encoding="ISO-8859-1").read()
        assert(text_data == out)


def test_count_processed_customers():
    count = solution1Spark.count_processed_customers()
    assert(count == 10)


def test_processed_customers_df():
    text_data = solution1Spark.get_processed_customers_df()
    try:
        out = open("tests/processed-customers","r").read()
        assert(text_data == out)
    except:
        out = open("tests/processed-customers","r", encoding="ISO-8859-1").read()
        assert(text_data == out)

