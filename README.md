Spark Dataset analyzer ![Build Status](https://travis-ci.org/GRpro/spark_dataset_analyzer.svg?branch=master)
-----------------------------

Spark application to read CSV dataset, parse it and provide some statistics

### Run standalone

Build the jar
`sbt build`

Run analyzer with sample.csv
`java -jar target/scala-2.11/analyzer.jar $( cat config.txt )`
