import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3.AmazonS3Client

import com.typesafe.scalalogging.slf4j.LazyLogging
import scala.collection.JavaConversions._

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet

import eu.bitwalker.useragentutils.UserAgent
import eu.bitwalker.useragentutils.Browser
import java.text.SimpleDateFormat

object TopBrowser extends App with LazyLogging {
  println("Loading")

  // S3 configuration
  val s3Credentials = new DefaultAWSCredentialsProviderChain().getCredentials
  val s3Endpoint = "s3.amazonaws.com"
  val amplabS3Bucket = "big-data-benchmark"
  val dataSetName = "uservisits"
  val dataSetSize = "tiny"
  val amplabS3Path = s"pavlo/text/$dataSetSize/$dataSetName"
  val filesToLoad = 10

  // MySQL configuration
  val mysqlUser = "spark_driver"
  val mysqlPass = "spark_driver_pa5sw0rd"
  val mysqlPath = "jdbc:mysql://localhost:3306/spark_results"

  // Spark configuration
  val conf = new SparkConf(true).setAppName(getClass.getSimpleName)
  val sparkContext = new SparkContext(conf)
  val hadoopCfg = sparkContext.hadoopConfiguration
  hadoopCfg.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
  hadoopCfg.set("fs.s3n.awsAccessKeyId", s3Credentials.getAWSAccessKeyId)
  hadoopCfg.set("fs.s3n.awsSecretAccessKey", s3Credentials.getAWSSecretKey)

  // List S3 files to load
  val s3Client = new AmazonS3Client(s3Credentials)
  s3Client.setEndpoint(s3Endpoint)

  val s3Files = s3Client
      .listObjects(amplabS3Bucket, amplabS3Path)
      .getObjectSummaries.toList
      .map(s => s.getKey)
      .filter(!_.contains("$folder$"))
      .map(key => s"s3n://$amplabS3Bucket/$key")

  println("Processing data")

  val browserUsage = sparkContext.union(s3Files.map(sparkContext.textFile(_)))
      .map(l => {
        val line = l.split(",")
        val date = new SimpleDateFormat("yyyy-MM").parse(line(2))
        val browser = new UserAgent(line(4)).getBrowser().toString

        ((date, browser), 1)
      })
      .filter(_._1._2 != "UNKNOWN")
      .reduceByKey(_ + _)
      .map(d => {
        val browser = d._1._2
        val visits = d._2
        val month = new SimpleDateFormat("yyyy-MM").format(d._1._1).toString
        (month, (browser, visits))
      })
      .groupByKey()
      .sortByKey(false)
      .cache()
  
  println("Last twenty months:")
  browserUsage.take(20).foreach(println)

  println(s"Saving to MySQL with path $mysqlPath")
  val conn = DriverManager.getConnection(mysqlPath, mysqlUser, mysqlPass)  

  try {
    val stm = conn.createStatement

    browserUsage.collect.foreach( line => {
      val date = line._1
      line._2.foreach( browser => {
        val name = browser._1
        val visits = browser._2
   
        stm.executeUpdate(s"INSERT INTO browser_usage (visit_date, name, visits) VALUES (STR_TO_DATE('$date', '%Y-%m'), '$name', '$visits')")
      })
    })
  }
  finally {
    conn.close
  }

  println("Done")
  sparkContext.stop()
}
