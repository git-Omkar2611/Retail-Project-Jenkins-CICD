#This file is used for writing fixtures.
import pytest
from lib import Utils

#Fixture is used for defining all setup related stuffs.
@pytest.fixture
def spark():
    "Creates Spark Session"
    sparkSession = Utils.get_spark_session("LOCAL")
    yield sparkSession
    "Terminating Spark Session"
    sparkSession.stop() #added to release resources

@pytest.fixture
def expected_results(spark):
    "Gives Expected Results from test_results"

    result_schema = "state string , count int"

    return spark.read.csv("data/test_results/state_aggregated.csv" , header = True , schema = result_schema)