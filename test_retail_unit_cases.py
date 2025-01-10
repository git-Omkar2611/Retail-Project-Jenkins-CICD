from lib import ConfigReader,DataManipuation,DataReader,Utils
import pytest
##Here spark is read from fixtures since the framework is smart enough to read it from conftest.py

@pytest.mark.skip("Work In Progress")
def test_read_customers(spark):
    customer_df = DataReader.read_customers(spark,"LOCAL")
    assert customer_df.count() == 12435

@pytest.mark.skip("Work In Progress")
def test_read_orders(spark):
    orders_df = DataReader.read_orders(spark,"LOCAL")
    assert orders_df.count() == 68884

@pytest.mark.skip("Work In Progress")
@pytest.mark.transformation() #Labelling to execute specific pytest
def test_filter_closed_orders(spark):
    orders_df = DataReader.read_orders(spark,"LOCAL")
    filtered_orders = DataManipuation.filter_closed_orders(orders_df)
    assert filtered_orders.count() > 7000

@pytest.mark.skip("Work In Progress")
def test_read_app_config():
    config = ConfigReader.get_app_config("LOCAL")
    assert config['customers.file.path'] == 'data/customers.csv'

@pytest.mark.skip("Work In Progress")
@pytest.mark.transformationaggregation()
def test_count_orders_state(spark , expected_results):
    customer_df = DataReader.read_customers(spark,"LOCAL")
    actual_results = DataManipuation.count_orders_state(customer_df)
    print(actual_results)
    assert actual_results.collect() == expected_results.collect()

@pytest.mark.parametrize("status,count",[
    ("CLOSED",7556),
    ("PENDING_PAYMENT" , 15030),
    ("COMPLETE" , 22900)
    ]
)
def test_check_count(spark,status,count):
    orders_df = DataReader.read_orders(spark,"LOCAL")
    filtered_orders = DataManipuation.filter_closed_orders_by_status(orders_df,status)
    assert filtered_orders.count() == count    