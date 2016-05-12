
  package com.ctm.sparkwordcount

  import java.io.FileInputStream
  import java.util.Properties

  import org.apache.log4j.{Level, LogManager}
  import org.apache.spark.{SparkConf, SparkContext}

  object SparkWordCount {
    var prop = new Properties()
    val logger = LogManager.getRootLogger
    logger.setLevel(Level.INFO)
      def main(args: Array[String]) {
        try {
          prop.load(new FileInputStream("src/main/resources/job.properties"))
        } catch { case e: Exception =>
          logger.error("Spark Wordcount template properties file not found" + e.printStackTrace())
        }

        val appName = prop.getProperty("appName")
        val localCores = prop.getProperty("localCores")
        val uiPort = prop.getProperty("uiPort")
        val conf = new SparkConf()
        conf.set("spark.app.name", appName)
        conf.set("spark.master", localCores)
        conf.set("spark.ui.port", uiPort) // Override the default port

        // Create a SparkContext with this configuration
        val sc = new SparkContext(conf)
        val inputFile =  prop.getProperty("inputFile")
        val textFile = sc.textFile(inputFile)
      val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
      counts.foreach(println)
      counts.take(50).foreach(println)
        val cnt = counts.count()
        logger.info("Count for the total number of words in the whole file is " + cnt)
    }
  }
