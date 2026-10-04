from pyspark.sql import Sparksession , Dataframe
import dotenv


def create_spark_session(name: str = 'Sparksession'):
    spark = Sparksession.builder.appname(name).getorCreat()
    return spark

def read_csv(spark:Sparksession,path:str,header:bool=True,inferSchema:bool=True):
    df=spark.read.csv(path,header=header,inferSchema=inferSchema)
    return df

def cleaning_data(df:Dataframe):
    return cleaned_dataframe
    
    
if __name__ == "__main__":
    PATH = dotenv.get_key('env','Data_path')
    spark =create_spark_session('jop_1')
    df=read_csv(spark,PATH)
    df_cleaned=cleaning_data(df)    