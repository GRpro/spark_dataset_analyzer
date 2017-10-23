package com.gr.analytics

import java.io.PrintWriter

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

object Application {

  def main(args: Array[String]): Unit = {
    val config = ArgParser.config(args(0))

    implicit val sparkSession = SparkSession.builder()
      .config("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
      .master(config.master.getOrElse("local[*]"))
      .appName("Spark dataset analyzer")
      .getOrCreate

    import sparkSession.implicits._

    def sourceDataFrame(config: Config)(implicit sparkSession: SparkSession): DataFrame = {
      var dfReader = sparkSession.read.format("com.databricks.spark.csv")
      config.csvOptions.foreach(entry => dfReader = dfReader.option(entry._1, entry._2))
      dfReader
        .option("header", "true")
        .option("inferSchema", "true")
        .load(config.sourcePath)
    }

    val df = sourceDataFrame(config)

    def filterFunction(values: String*) = values.forall(value =>
      value == null || value.trim.nonEmpty)

    // filter rows with columns of String type where values are empty strings or spaces
    sparkSession.udf.register("filterFunction", filterFunction _)
    val stringColumnNames = df.schema.filter(_.dataType == StringType).map(_.name)
    val filteredDf = df.filter(callUDF("filterFunction", array(stringColumnNames.map(col): _*)))

    // filteredDf.show()

    // convert columns due to converter settings
    val castedDf = filteredDf.select(config.conversions.map(Converter.convert): _*)

    // castedDf.show()

    case class ColumnStats(Column: String, Unique_values: Long, Values: List[Map[String, Long]])

    import spray.json.DefaultJsonProtocol._
    import spray.json._

    implicit val columnStatsFormat = jsonFormat3(ColumnStats)

    val statistics = castedDf.columns.map(column => {
      val uniqueDf = castedDf
        .select(col(column))
        .where(col(column).isNotNull)
        .groupBy(col(column)).count()

      // uniqueDf.show()

      val list = uniqueDf.collect()
        .map(row => Map(row.get(0).toString -> row.getAs[Long](1))).toList
      ColumnStats(column, list.size, list)
    })

    val statisticsJson = statistics.toJson.prettyPrint

    new PrintWriter(config.destPath) {
      write(statisticsJson)
      close()
    }

//    sparkSession.sparkContext.parallelize(Seq(statisticsJson), 1).toDF().write.format("com.databricks.spark.csv").save(config.destPath)

  }
}
