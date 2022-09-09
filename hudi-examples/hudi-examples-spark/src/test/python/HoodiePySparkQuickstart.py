import sys
from pyspark import sql
from datetime import datetime
import random
from py4j.java_gateway import java_import


DEFAULT_FIRST_PARTITION_PATH = "2020/01/01"
DEFAULT_SECOND_PARTITION_PATH = "2020/01/02"
DEFAULT_THIRD_PARTITION_PATH = "2020/01/03"
DEFAULT_PARTITION_PATHS = [DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH]


def generateGenericRecord(uuid, riderName, driverName, timestamp):
    return sql.Row(uuid=uuid, ts=timestamp,rider=riderName,driver=driverName,
    begin_lat=random.random(),begin_lon=random.random(),end_lat=random.random(),
    end_lon=random.random(),fare=random.random()*100)

def insertData(spark, tablename):
    dataGen = spark._jvm.org.apache.hudi.QuickstartUtils.DataGenerator()
    inserts = spark._jvm.org.apache.hudi.QuickstartUtils.convertToStringList(dataGen.generateInserts(10))
    print(inserts.size())
    df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
    commitTime = datetime.now().strftime("%H:%M:%S")
    print(df.count())
    print("hello world")

    return


def runQuickstart(spark, tablename):
    insertData(spark,tableName)

    return




if __name__ == "__main__":
    random.seed(46474747)
    if len(sys.argv) < 2:
        print("Usage: HoodiePySparkQuickstart <tablePath> <tableName>")
        quit(-1)
    tableName = sys.argv[1]
    spark = sql.SparkSession \
        .builder \
        .appName("Hudi Spark basic example") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer.max", "512m") \
        .getOrCreate()
    java_import(spark._sc._jvm, "org.apache.spark.sql.api.python.*")
    runQuickstart(spark,tableName)




