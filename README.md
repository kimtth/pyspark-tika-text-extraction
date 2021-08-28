
# Apache Tika + PySpark
 
 Text extraction performance tuning results for a huge amount of files. The assessment conducted by combination with Apache Tika & PySpark & Multiprocessing.
 
 1. Apache Tika is a content detection and text extraction framework, written in Java.
 
 2. Apache Spark is written in Scala programming language. PySpark is an interface for Apache Spark in Python.
 
 - Project Structure
 
   + Dependency
   
   ```
   tika-jar: tika-server.jar v1.24
   spark: spark-3.1.2-bin-hadoop2
   hadoop: hadoop_winutils_2.7
   dummy: test-data-storage
   ```
   
   + Apache-Spark: All spark configurations have been already incorporated in [tika_with_spark.py].
   
   ```python
   os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jre1.8.0_201'
   os.environ["SPARK_HOME"] = os.path.abspath(r"./spark/spark-3.1.2-bin-hadoop2.7")
   os.environ["HADOOP_HOME"] = os.path.abspath(r"./hadoop")
   os.environ["PATH"] += os.path.abspath(r"./spark/spark-3.1.2-bin-hadoop2.7/bin")
   os.environ["PATH"] += os.pathsep + os.path.abspath(r"./hadoop/bin")
   ```
   
   + tika-python: This API is binding to Apache Tika REST services, At the initial, API launches a tika-server instance, the extraction request will consume and process in the server. If you try tika-app.jar instead of tika-server.jar, it is not going to be worked. 
   https://github.com/chrismattmann/tika-python
   
   + Application Code
   
   ```
   dummy_data_gen.py: Test file Generator
   tika_with_spark.py: Pyspark and Tika Integration Code
   tika_without_spark.py: Tika code for Single and Multi Thread 
   ```
   
 - The description of sample file for testing 
    
    ```
    CISA-CPE_bro_Kor_0415.pdf | Korean | 400,768 bytes * 50
    strata_spark_streaming.ppt | English | 4,847,616 bytes * 50
    ```
    Entire files: 100 files / 250 MB
    
 - Apache Tika + PySpark
 
   ```bash
   --- Text Extraction Started 0.0 seconds ---
   --- Completed 21.888809204101562 seconds ---
   --- Csv Export Completed 95.74139404296875 seconds ---
   ```

 - Single Thread (Without PySpark)
 
   ```bash 
   --- Step 1 1.1718764305114746 seconds ---
   --- Text Extraction 100 files took 231.43276596069336 seconds ---
   --- Step 2 231.6859369277954 seconds ---
   ```
 
 - Multi-Threads (Without PySpark)
 
    ```bash
    --- Step 1 0.6341872215270996 seconds ---
    --- Text Extraction 100 files took 196.48454070091248 seconds ---
    --- Step 2 199.58791089057922 seconds ---
    ```

 #### In Conclusion: 
 
 Multi-Threads (Without Pyspark) could not make significant enhancement of text extraction performance. In some cases, Multi-Threads (Without Pyspark) will make frequently context-switching, this will result in latency and poor performance than a single thread.
 Only along with Pyspark, you will achieve the best performance with remarkable enhancement.
