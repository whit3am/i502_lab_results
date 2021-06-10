import pyspark as ps
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType


class SqlQuery:
    """
    Create a sql query w/ adjustable year and month parameters.

    Parameters
    ----------
    year: int
            the lowest year data is requested for (inclusive).
    month: int
            the lowest month data is requested for (inclusive).
    sql_query: str
            a sql query with year and month

    Examples
    --------
    q = ('''SELECT global_id
        FROM lab_tests
        WHERE created_at > CAST("{}-{}-01" AS DATE)
        ''')
    IN: print(query_1 = SqlQuery(q, 2020, 1))
    OUT: SELECT global_id
         FROM lab_tests
         WHERE created_at > CAST("2020-1-01" AS DATE)

    """
    def __init__(self, sql_query: str, year: int, month: int):
        self.year = year
        self.month = month
        self.query = sql_query
        self.formatted_query = sql_query.format(year, month)

    def __repr__(self):
        return self.formatted_query


class DataPipe:
    """
    This class acts as a wrapper for pyspark and was built for WA I-502 public data.

    This data pipeline was built to extract smaller subsets of I-502 data. This class could be abstracted out by
    removing some of the hard-coding and replacing them with kwargs; however, this dataset was large enough that
    this solution seemed warranted. The general workflow is to target an I-502 csv, pass a sql query, and profit.
    This pipeline attempts to first run the exact query and turn that into a csv. If that fails the pipeline will
    reattempt by pulling data at the monthly level. This significantly slows the process, but reduces Java heap
    errors.
    """
    def __init__(self, data_path: str, sql_query: SqlQuery, filename: str):
        self.data_path = data_path
        self.sql_query = sql_query
        self.spark = self.build_spark()
        self.sc = self.build_spark_context()
        self.spark_df = self.read_data(data_path)
        self.temp_view = self.spark_df.createOrReplaceTempView('temp')

        self.clean_i_502()
        try:
            self.query_results = self.execute_query(sql_query)
            self.to_csv(self.query_results, filename)
        except:  # bad--need to narrow this
            for year in range(sql_query.year, 2021):
                for month in range(sql_query.month, 13):
                    sql_query = SqlQuery(sql_query.query, year, month)
                    result = self.execute_query(sql_query)
                    self.to_csv(result, filename)

    @staticmethod
    def build_spark() -> ps.sql.SparkSession:
        """
        Builds the spark session object
        :return: spark.sql.SparkSession
        """
        spark = (ps.sql.SparkSession.builder
                 .master('local[8]')
                 .appName('sparkSQL')
                 .getOrCreate())
        spark.conf.set("spark.debug.maxToStringFields", "9999")
        spark.conf.set("spark.debug.maxPlanStringLength", "9999")
        return spark

    def build_spark_context(self) -> ps.sql.SparkSession.sparkContext:
        """
        Builds the spark context object
        :return: ps.sql.SparkSession.sparkContext
        """
        spark = self.spark
        sc = spark.sparkContext
        sc.setLogLevel("INFO")
        return sc

    def read_data(self, data_path):
        """
        This method reads in the targeted I-502 with hardcoded params to ensure data is read in properly.
        Parameters
        ----------
        data_path str
            The path absolute or relative to the target I-502 csv.

        Returns
        -------
        spark.sql.DataFrame object for our targeted csv.
        """
        spark = self.spark
        df = spark.read.options(header='true', inferSchema='true', sep='\t', encoding='UTF-16') \
                  .csv(data_path)
        return df

    def clean_i_502(self):
        """
        Performs needed data cleaning. The only known cleaning needed is removing the UTF error code character "�"
        Creates DAG to alter data in place.
        """
        df = self.spark_df
        df = df.filter(~col(df.columns[0]).like("%�%"))
        remove_last_char_in_str = udf(lambda x: x[:-1], StringType())
        df = (df.withColumn(df.columns[-1], remove_last_char_in_str(df.columns[-1])))
        self.spark_df = df

    def execute_query(self, sql_query=None):
        """
        Executes the provided sql query.

        Parameters
        ----------
        sql_query: SqlQuery or None
            If None--defaults to the original sql query given. The only reason an additional sql query is given is
            to break up the date range after a Java Heap error--this tends to be from trying to collect too much data
            at once.

        Returns
        -------
        spark.sql.DataFrame object defined by our data and the sql query that is ran against that data.
        """
        sql_query = sql_query if sql_query else self.sql_query
        month = sql_query.month
        year = sql_query.year
        query = sql_query.query.format(year, month)
        result = self.spark.sql(query)
        return result

    @staticmethod
    def to_csv(result, filename, year=None, month=None):
        """
        Parameters
        ----------
        result: ps.sql.DataFrame
            Spark dataframe result from sql query.
        filename: str
            Filename to name the output csv.
        year: str or None
            Year used in the sql query--used to append to name.
        month: str or None
            Month used in the sql query--used to append to name.
        Returns
        -------
        None--uploads a csv to the scripts location with the given name.

        """
        if year is month is None:
            path = f'{filename}.csv'
        else:
            path = f'{filename}_year_{year}_month_{month}.csv'
        result.coalesce(1).write.csv(path, header='true')


def main():
    pass


if __name__ == '__main__':
    main()
