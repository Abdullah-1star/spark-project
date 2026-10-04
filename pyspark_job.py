from pyspark.sql import Sparksession , Dataframe
from pyspark.sql import functions as F
import dotenv


def create_spark_session(name: str = 'Sparksession'):
    return Sparksession.builder.appname(name).getorCreat()
     

def read_json(spark:Sparksession,path:str,header:bool=True,inferSchema:bool=True):
    return (spark.read.option("multiline", "true").json(path))
    

def cleaning_data(df:Dataframe):
    
    return (
        df.filter(F.col("amount") > 0)
        .filter(F.col("name").isNotNull())
        .withColumn('amount_with_tax',F.col('amount')*1.20)
    )
    
    
    
if __name__ == "__main__":
   import dotenv

   path = dotenv.get_key(".env", "Data_path")
   spark = create_spark_session("job_1")
   df = read_json(spark, path)
   cleaning_data(df).show()   