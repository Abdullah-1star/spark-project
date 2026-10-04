import pytest
from pyspark.sql import SparkSession

from pyspark_job import clean_data


@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    yield s
    s.stop()


def make_df(spark, rows):
    return spark.createDataFrame(rows, "name string, amount double")


def test_valid_records_are_kept(spark):
    df = make_df(spark, [("Ali", 100.0), ("Sara", 50.0)])
    assert clean_data(df).count() == 2


def test_non_positive_amount_removed(spark):
    df = make_df(spark, [("A", 0.0), ("B", -5.0), ("C", 10.0)])
    names = [r["name"] for r in clean_data(df).collect()]
    assert names == ["C"]


def test_null_name_removed(spark):
    df = make_df(spark, [(None, 100.0), ("Ali", 100.0)])
    names = [r["name"] for r in clean_data(df).collect()]
    assert names == ["Ali"]


def test_amount_with_tax_calculated(spark):
    df = make_df(spark, [("Ali", 100.0), ("Sara", 50.0)])
    rows = {r["name"]: r["amount_with_tax"] for r in clean_data(df).collect()}
    assert rows["Ali"] == pytest.approx(120.0)
    assert rows["Sara"] == pytest.approx(60.0)