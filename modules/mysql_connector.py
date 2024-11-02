from pyspark.sql import SparkSession


class MySQLConnector:
    def __init__(self, spark: SparkSession, connection_properties: dict, url: str):
        self.spark = spark
        self.properties = connection_properties
        self.url = url

    def get_dataframe(self, sql_query: str):
        """
        Execute a SQL query and return the result as a Spark DataFrame.

        Parameters:
        sql_query (str): SQL query to be executed.

        Returns:
        DataFrame: Resultant DataFrame from the executed SQL query.
        """
        df = self.spark.read.jdbc(
            url=self.url,
            table=sql_query,
            properties=self.properties
        )
        return df
