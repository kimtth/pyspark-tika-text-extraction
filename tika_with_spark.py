import io
import os
import time

from pyspark.sql.types import StringType

os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jre1.8.0_201'
os.environ["SPARK_HOME"] = os.path.abspath(r"./spark/spark-3.1.2-bin-hadoop2.7")
os.environ["HADOOP_HOME"] = os.path.abspath(r"./hadoop")
os.environ["PATH"] += os.path.abspath(r"./spark/spark-3.1.2-bin-hadoop2.7/bin")
os.environ["PATH"] += os.pathsep + os.path.abspath(r"./hadoop/bin")
print(os.getenv('PATH'))

from tika import parser, tika
import findspark

findspark.init()

# Tika Server Mode / Client mode is enough for processing.
# 1. java -jar tika-server.jar -h 0.0.0.0
# 2. tika.TikaClientOnly = True
# Ref: cmd> netstat -ano | findstr :<PORT>
#      cmd> taskkill /PID <PID> /F
TIKA_SERVER_JAR = '.\\tika-jar'
tika.TikaJarPath = TIKA_SERVER_JAR  # tika-server-1.24

# import pyspark only run after findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import Row

# spark = SparkSession.builder.getOrCreate()
spark = SparkSession.builder.appName('spark-tika').getOrCreate()
df = spark.sql('''select 'spark' as hello ''') # Hello Message
df.show()

destination_dir = ".\\dummy\\file-data-load"


def file_list_gen(file_list_dir):
    list_files = []
    file_list = os.listdir(file_list_dir)
    for file_key in file_list:
        file_location = os.path.join(file_list_dir, file_key)
        list_files.append(file_location)

    return list_files


def extract_content(file_location):
    abs_file_path = os.path.abspath(file_location)
    with open(abs_file_path, mode='rb') as file:
        binary = io.BytesIO(file.read())

    parsed = parser.from_buffer(binary)  # , 'http://127.0.0.1:9998/tika')
    # print(parsed["metadata"])
    if parsed["content"]:
        return ' '.join(parsed["content"].split())
    else:
        return ''


# Dict to Pandas Dataframe
# df = pd.DataFrame(file_list_dict.items(), columns=['path', 'content'])
# Pandas Dataframe to Spark Dataframe
# ddf = spark.createDataFrame(df)

file_list_from_archive = file_list_gen(destination_dir)
ddf2 = spark.createDataFrame(file_list_from_archive, StringType())
print('1:count', ddf2.count(), '2:column size', len(ddf2.columns))

start_time = time.time()
print("--- Text Extraction Started %s seconds ---" % (time.time() - start_time))
# x[0] : Extract the string value from PySpark Row
src = ddf2.rdd.map(
    lambda x: (extract_content(x[0])))

# SparkSession.createDataFrame, which is used under the hood,
# requires an RDD / list of Row/tuple/list/dict* or pandas.DataFrame, unless schema with DataType is provided.
df2 = src.map(lambda x: Row(x)).toDF()
df2.show(5, True)

# Save DF to Txt
abs_file_path = os.path.abspath(".\\output\\output.csv")
# UTF-8 BOM, Excel supports UTF-8 BOM as a default encoding.
print("--- Completed %s seconds ---" % (time.time() - start_time))
df2.toPandas().to_csv(abs_file_path, encoding='utf-8-sig')
print("--- Csv Export Completed %s seconds ---" % (time.time() - start_time))
