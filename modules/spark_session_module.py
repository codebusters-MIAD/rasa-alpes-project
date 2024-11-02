# spark_session_module.py

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession


def create_spark_session(path_jar_driver):
    # Configuración de la sesión de Spark
    conf = SparkConf() \
        .set('spark.driver.extraClassPath', path_jar_driver)

    spark_context = SparkContext(conf=conf)
    sql_context = SQLContext(spark_context)
    spark = sql_context.sparkSession

    return spark
