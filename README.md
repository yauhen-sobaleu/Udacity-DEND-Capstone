## Project overview

During this project we will read in an sqlite database with USA wildfires data together with a temperature outliers dataset using Apache Spark and store resulting data in a datalake on s3. The data model of the project is to create a star-schema pattern with resulting tables and store it on s3 in corresponding buckets with Amazon Athena on top for ad-hoc querying. Resulting data can be used to analyze geo-spatial information from both datasets to find correlation between unusual weather conditions and the probability of occuring a wildfire by superimposing coordinates data from both datasets. Also it can be used for measuring the efficiency of containing wildfires by responsible agencies.

We will use Scala API of Apache Spark for this project with an sbt-assembly "fat" jar as a final deliverable. Choice of language is dictated by perfomance benefits of using Scala as a JVM language that is native to Spark.

## Project structure
```
├── athena
│   ├── athena.ipynb
│   ├── athena_ddl.py
│   └── create_athenadb.py
├── build.sbt
├── capstone.ipynb
├── project
│   ├── assembly.sbt
│   └── build.properties
├── src
│   └── main
│       ├── resources
│       │   └── application.conf
│       └── scala
│           └── com
│               └── dend
│                   └── capstone
│                       ├── Capstone.scala
│                       └── utils
│                           ├── sparkModelling.scala
│                           └── sparkUtils.scala
└── target
    └── scala-2.11
        └── CapstoneDEND-assembly-0.1.jar
```

## Project EDA
Open `capstone.ipynb` to see details of exploratory data analysis of both datasets

## How to run
To run this project you need to have Spark and Scala installed on your pc:
* Spark 2.4.0
* Scala 2.11.12

### Steps
1. Download [1.88 Million US Wildfires](https://www.kaggle.com/rtatman/188-million-us-wildfires) and [U.S. Weather Outliers](https://data.world/carlvlewis/u-s-weather-outliers-1964) datasets to your local pc
2. Clone the repository
3. Edit `src/main/resources/application.conf` file to set your AWS credentials and paths to datasets:
   1. `spark.wildfirespath` - path to usa wildfires sqlite database (e.g. /Users/superuser/Downloads/FPA_FOD_20170508.sqlite)
   2. `spark.weatherpath` - path to weather outliers csv file
   3. `spark.rootbucket` - S3 bucket where spark application will save data (e.g. capstonebucket)
   4. `spark.awskey` - your AWS key
   5. `spark.awssecret` - your AWS secret
   6. `athena.dbname` - name of athena database that will be create by `create_athenadb.py` (e.g. capstonedb)
   7. `athena.logsoutputbucket` - s3 folder in the root bucket where Amazon Athena client will output its logs (e.g. athena_logs)
4. Run `which spark-submit` to know where spark-submit script is located on your pc
5. Run `spark-submit` on the .jar file included in the repository specifying `application.conf` file as a source of configurations (e.g. ```/Users/superuser/bin/spark-submit --master local --properties-file /Users/superuser/Udacity-DEND-Capstone/src/main/resources/application.conf /Users/superuser/Udacity-DEND-Capstone/target/scala-2.11/CapstoneDEND-assembly-0.1.jar ```) and wait until it's complete
6. Open `athena/athena.ipynb` jupyter notebook and run all cells to create a database with 11 tables and perform quality checks