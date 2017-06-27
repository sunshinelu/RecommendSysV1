package com.ecloud.Inglory.Test

import com.github.seratch.scalikesolr.HttpSolrServer
import org.apache.hadoop.record.Record
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/3/1.
 */
object solrTest2 {
  def main(args: Array[String]) {
    def SetLogger = {
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("com").setLevel(Level.OFF)
      System.setProperty("spark.ui.showConsoleProgress", "false")
      Logger.getRootLogger().setLevel(Level.OFF)
    }
    def main(args: Array[String]) {
      SetLogger
      //bulid environment
      val spark = SparkSession.builder.master("local").appName("AlsEvaluation").getOrCreate()
      val sc = spark.sparkContext

      val csvFileLocation = "/Users/sunlu/Desktop/Test/nyc_yellow_taxi_sample_1k.csv"
      val csvDF = spark.read.format("com.databricks.spark.csv").
        option("header", "true").
        option("inferSchema", "true").
        load(csvFileLocation)

      val df1 = csvDF.select("vendor_id", "pickup_datetime", "store_and_fwd_flag", "total_amount")
      val rdd1 = df1.rdd.map(row => (row(0), row(1), row(2), row(3)))




      val solrUrl = "http://localhost:8983/solr/my_collection"
      //val server = new HttpSolrServer(solrUrl)






      val options = Map(
        "zkhost" -> "localhost:2181",
        "collection" -> "my_collection",
        "gen_uniq_key" -> "true" // Generate unique key if the 'id' field does not exist
      )
      // Write to Solr
      csvDF.write.format("solr").options(options).mode(org.apache.spark.sql.SaveMode.Overwrite).save

    }
  }
}

