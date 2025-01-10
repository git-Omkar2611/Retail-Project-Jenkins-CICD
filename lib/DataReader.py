#This File is responsible to create spark dataframes
from lib import ConfigReader

def get_customer_schema():
    customer_schema =  "customer_id long ,customer_fname string ,customer_lname string ,username string ,password string ,address string ,city string ,state string ,pincode string"

def get_orders_schema():
    orders_schema = "order_id long , order_date timestamp , customer_id long , status string"
    return orders_schema

def read_customers(spark,env) :
    conf = ConfigReader.get_app_config(env)
    customers_file_path = conf["customers.file.path"]
    return spark.read.csv(customers_file_path , header = True , schema = get_customer_schema())

def read_orders(spark,env) :
    conf = ConfigReader.get_app_config(env)
    orders_file_path = conf["orders.file.path"]
    return spark.read.csv(orders_file_path , header = True , schema = get_orders_schema())