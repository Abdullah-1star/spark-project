from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def create_spark_session(name: str = "SparkSession") -> SparkSession:
    return SparkSession.builder.appName(name).getOrCreate()


def read_json(spark: SparkSession, path: str, multiline: bool = False) -> DataFrame:
    return spark.read.option("multiLine", multiline).json(path)


def clean_data(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("amount") > 0)
        .filter(F.col("name").isNotNull())
        .withColumn("amount_with_tax", F.col("amount") * 1.20)
    )


if __name__ == "__main__":
    import dotenv

    path = dotenv.get_key(".env", "Data_path")
    spark = create_spark_session("job_1")
    df = read_json(spark, path)
    clean_data(df).show()