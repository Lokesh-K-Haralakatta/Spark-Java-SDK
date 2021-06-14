# Spark-Java-SDK
###### Spark Java SDK helps developers to learn and use various [Apache Spark](https://spark.apache.org/docs/latest/index.html) Modules Programming.

### About Spark-Java-SDK
Spark-Java-SDK is package of java spark applications for various [Apache Spark](https://spark.apache.org/docs/latest/index.html) Modules using which a developer can use these spark applications grouped under various packages according to Spark modules:
   - To learn about how to use Spark Module functionalities in writing spark programs in java to program spark modules using various transaformations and actions.
   - To consume Spark-Java-SDK in their project to make use of underlying spark modules operations in their applications.
   - Developers can add in their own required functionality into respective spark module packge while consuming Spark-Java-SDK.
   - Apark-Java-SDK can be executed on local environment and also on cluster environment like [Spark Standalone Cluster Mode](https://spark.apache.org/docs/latest/spark-standalone.html)

### Prerequisites
   - [Apache Spark](https://spark.apache.org/docs/latest/index.html)
   - [Apache Maven](https://maven.apache.org/)
   - [Java 8](https://www.oracle.com/in/java/technologies/javase/javase-jdk8-downloads.html)
   - Linux / Mac OS

### Apache Spark Setup
   - Download appropriate [Apache Spark]((https://spark.apache.org/docs/latest/index.html) binary archive from [Apache Spark Downloads Page](https://spark.apache.org/downloads.html)
   - While downloading select Apache Spark version 3.1.1 / later and pre-built with hadoop version 2.7
   - Extract the archive contents to file system, say `/opt/spark-3.1.1-bin-hadoop2.7`
   - Define shell variable as `export SPARK_HOME="/opt/spark-3.1.1-bin-hadoop2.7"`
   - In order to work with [Spark Standalone Cluster Mode](https://spark.apache.org/docs/latest/spark-standalone.html),
     - Start master using the command `$SPARK_HOME/sbin/start-master.sh`
     - Start worker node using the command `$SPARK_HOME/sbin/start-worker.sh sparkURL`

### Supported Spark Modules and Functionality
   - ##### [Apache Spark Core RDD API](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
      - ##### [Parallelized Collections](https://spark.apache.org/docs/latest/rdd-programming-guide.html#parallelized-collections)
        The java class [RddParalellize.java](https://github.com/Lokesh-K-Haralakatta/Spark-Java-SDK/develop/src/main/java/com/loki/spark/rdd/RddParalellize.java) present in the SDK contains various methods to perform computations using different [RDD operations](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-operations) on parallelized collection. Below listed are the various functionalities provided:
        - **computeEvenSum:** Uses filter transformation and reduce action on parallelized collection to compute sum of all even numbers present in collection with 2 partitions.
        - **computeOddSum:** Uses filter transformation and reduce action on parallelized collection to compute sum of all odd numbers present in collection with 2 partitions.
        - **computeEachItemCount:** Uses map and reduceByKey transformations to count each integer present in the collection and collects result in a map.
        - **computeEachDigitCount:** Uses flatMap, map and reduceByKey transformations to count digits of each integer present in the collection, collects result in a map.
        - **computeEachDigitSum:** Uses flatMap, map, groupByKey, mapToPair transformations to compute each digit sum present in collection integers and saves the result using saveAsTextFile into given directory. 
      - ##### [External Datasets](https://spark.apache.org/docs/latest/rdd-programming-guide.html#external-datasets)

### Steps to build Spark-Java-SDK
   - Get the sdk's source either using `git clone` or download the source
   - Execute `mvn clean package -DskipTests` from the source directory
   - We should have the jar built and placed under `target` directory on successfull build

### Steps to run tests
   - Make sure we have Spark-Java-SDK jar successfully built and present in `target` directory
   - Tests can be executed either in `LOCAL` or `CLUSTER` Modes
   - To run tests in local mode, execute `mvn test` from source directory
   - For cluster mode, update required properties in the file [TestUtilMethods.java](https://github.com/Lokesh-K-Haralakatta/Spark-Java-SDK/develop/src/test/java/com/loki/spark/test/util/TestUtilMethods.java) like
     - SPARK_CLUSTER
     - SPARK_HOME
     - JAR_NAME
   - We can also override above properties using JVM -D option in the run command below
   - To run tests in cluster mode, execute `mvn test -Denv=cluster` from source directory

### Spark-Java-SDK consumption as library in user programs

### Steps to build and run user programs with Spark-Java-SDK consumption

### Contribute
Spark-Java-SDK is going to be released under Apache 2.0 License. Interested developers/testers are welcome to contribute.
