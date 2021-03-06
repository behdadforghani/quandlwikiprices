package com.exapackets

import java.io._
import scala.collection.mutable.ListBuffer

import org.apache.spark.input.PortableDataStream
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{ State, Time }
import org.apache.spark.streaming.util.OpenHashMapBasedStateMap
import org.apache.spark.util.Utils
import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.functions.{ max, col }

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.log4j.Logger

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
 * Main object
 *
 */
object DownloadEod {

  val LOG = Logger.getLogger(getClass.getName)

  var checkPoint: String = null // File to store the date of the last download
  val APP_NAME = "End of the day stock price download"
  val dateFormat = "yyyy-MM-dd"

  var jdbcUrl: String = null
  var dbTable: String = null
  var dbUser: String = null
  var dbPass: String = null
  val env = sys.env

  def error(msg: String) {
    System.err.println(msg)
    LOG.error(msg)
    System.exit(1)
  }

  def usage() {
    error("usage: DownloadEod [start-date]");
    System.exit(1)
  }

  def loadData(csvFile: String, start: String) {
    val sparkConf = new SparkConf() //create a new spark config
      .setAppName(APP_NAME)
    val sc = new SparkContext(sparkConf) //build a spark context
    val spark = SparkSession
      .builder()
      .appName(APP_NAME)
      .getOrCreate()

    val df = spark.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ",").option("quote", "")
      .load(csvFile)

    // rename columns ex-dividend, date and open (reserved words) and open so that the users do
    // not need to escape the column names 
    val colMap = Map("ex-dividend" -> "ex_dividend", "open" -> "open_value", "date" -> "date_value")

    val dateFilteredDf = if (start != null) {
      //          df.filter(s"date > cast('${start}' as DATE)")
      println(s"date > '${start}'")
      df.filter(s"date > cast('${start}' as TIMESTAMP)")
    } else {
      df
    }
    val ndf =
      dateFilteredDf.select(df.columns.map(c => df.col(c).as(colMap.getOrElse(c, c))): _*)
    ndf.show()

    val connectionProperties = new Properties()
    connectionProperties.put("user", dbUser)
    connectionProperties.put("password", dbPass)
    connectionProperties.put("driver", "org.postgresql.Driver")
    ndf.write.mode("append").jdbc(jdbcUrl, dbTable, connectionProperties)

    val maxDf = ndf.agg(max("date_value")).withColumn("m", col("max(date_value)").cast("DATE")).select("m").rdd.collect()
    if (maxDf.length > 0) {
      val maxDate = maxDf(0)(0)
      println(s"maxDate=${maxDate}")
      val pw = new PrintWriter(new File(checkPoint))
      pw.write(s"${maxDate}")
      pw.close
    }

    sc.stop()
  }

  def getEnv(key: String): String = {
    val envVar = env.getOrElse(key, null).asInstanceOf[String]
    if (envVar == null) {
      throw new Exception("Environment variable " + key + " has not been set")
    }

    envVar
  }

  def main(args: Array[String]) {

    var key: String = null
    try {
      key = getEnv("QUANDL_KEY").toString()
      dbTable = getEnv("QUANDL_DB_TABLE").toString()
      dbUser = getEnv("QUANDL_DB_USER").toString()
      dbPass = getEnv("QUANDL_DB_PASS").toString()
      jdbcUrl = getEnv("QUANDL_JDBC_URL").toString()
      checkPoint = getEnv("QUANDL_CHECKPOINT").toString()
    } catch {
      case e: Throwable =>
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        error(e.toString)
        error(sw.toString)
    }

    var start: String = null
    args.size match {
      case 0 => {
        try {
          start = scala.io.Source.fromFile(checkPoint).mkString.trim()
          println(s"last update ${start}")
        } catch {
          case _: Throwable => LOG.warn("could not find checkpoint file will try to upload the whole database")
        }
      }
      case 1 => {
        start = args(0)
      }
      case _ => usage();
    }

    println("downloading data")
    val inputCsv = QuandlRest.fetchData(key)
    println("fetched data, uploading data to database")
    loadData(inputCsv, start)
    //  For testing    
    //        loadData("WIKI_PRICES_212b326a081eacca455e13140d7bb9db.csv", start)
  }
}
