from pyspark.sql import SparkSession
from pyspark.sql.functions import UserDefinedFunction


def CrawlerFinder():
    def __init__(self, data_path, tablename):

        self.spark = SparkSession.builder.appName("RobotIPIdentifier").getOrCreate()
        self.df = self.spark.read.csv(data_path, header=True)
        self.tablename = tablename

    def writeintodb(self, tablename):
        self.df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://mypqlinstance.cewczr0j0xld.us-east-1.rds.amazonaws.com:5432/my_insight_db") \
            .option("dbtable", self.tablename) \
            .option("driver", "org.postgresql.Driver") \
            .option("user", "mzhao15") \
            .option("password", "zhaomeng148097") \
            .mode("overwrite") \
            .save()

    def visitperday(self):
        totalvisit = self.df.groupBy('ip', 'date').count()
        totalvisit = visitperday.filter(totalvisit['count'] > 500).drop('count')
        self.writeintodb(totalvisit)

    def visitpermin(self):
        udf = UserDefinedFunction(lambda x: x[0:5])
        totalvisit = self.df.withColumn(
            self.df['time'], udf).groupBy('ip', 'date', 'time')
        self.writeintodb(totalvisit)

    def visitpercompanypermin():
        pass

    def run():
        pass


if __name__ == "__main__":
    data_path = "s3a://my-insight-data/logfiles2016/log20160101.csv"
    tablename = 'test'
    finder = CrawlerFinder(data_path, tablename)
