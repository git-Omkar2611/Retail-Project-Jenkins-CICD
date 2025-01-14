#This file is responsible for al pyspark transformations
from pyspark.sql.functions import *

def filter_closed_orders(orders_df):
    return orders_df.filter("status = 'CLOSED'")

def join_orders_customers(orders_df , customers_df) :
    return orders_df.join(customers_df , "customer_id")

def count_orders_state(joined_df) :
    return joined_df.groupBy('state').count()

def filter_closed_orders_by_status(orders_df,status):
    return orders_df.filter(col("status") == status)