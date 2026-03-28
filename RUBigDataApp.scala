package org.rubigdata


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.jsoup.Jsoup
import collection.JavaConverters._
import org.apache.spark.sql.functions._


object RUBigDataApp {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
        .setAppName("RUBigData WARC4Spark 2025")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._ 

    spark.sparkContext.setLogLevel("WARN")

    val extractTitle = udf( (html: String) => {
        val d = Jsoup.parse(html)
        val title=d.title()
        if(title!=null) title.toLowerCase else ""
    })

    val stripSourcePrefix = udf((title: String,src: String) => {
        val step1=title.replaceAll("(?i)\\bbbc(\\s+\\w+)?\\b\\s*[-:|]?\\s*", "")
        val step2=title.replaceAll("\\bcnn(\\s+\\w+)?\\b\\s*[-|:|\\|]?\\s*", "")
        
        src.toLowerCase match {
          case "bbc"  => step1.trim
          case "ecnn" => step2.trim
          case _      => title.trim
  }
    })

    def processWarc(source: String): DataFrame = {
      val warcPath = s"file:///opt/hadoop/rubigdata/${source}.warc.gz"

      val records = spark.read
        .format("org.rubigdata.warc")
        .option("parseHTTP", true)
        .load(warcPath)
        .filter($"warcType" === "response" && $"httpContentType".startsWith("text/html"))

      val titles = records
        .withColumn("title", extractTitle($"httpBody"))
        .filter(length($"title") > 0)
        .filter(!lower($"title").startsWith("redirecting"))
        .withColumn("title", stripSourcePrefix($"title", lit(source)))
        .withColumn("source", lit(source))

      titles
 
    }

    val bbcTitles = processWarc("bbc")
    val cnnTitles = processWarc("ecnn")

    val stopwords = Set("the", "and", "in", "of", "to", "on", "for", "with", "at", "a", "is", "us", "from","how","after")
    
    println("Top words from BBC:")
    val BBCwords=bbcTitles
        .select(explode(split(lower($"title"), raw"\W+")).as("word"))
        .filter(length($"word")>1)
        .filter(!col("word").isin(stopwords.toSeq: _*))
        .groupBy("word")
        .count()
        .orderBy(desc("count"))
        .limit(10)

    BBCwords.show(false)

    println("Top words from CNN:")
    val CNNwords=cnnTitles
        .select(explode(split(lower($"title"), raw"\W+")).as("word"))
        .filter(length($"word")>1)
        .filter(!col("word").isin(stopwords.toSeq: _*))
        .groupBy("word")
        .count()
        .orderBy(desc("count"))
        .limit(10)

    CNNwords.show(false)


    val combinedTitles = bbcTitles.union(cnnTitles)

    println("Top words overall:")
    val overallwords = combinedTitles
      .select(explode(split(lower($"title"), raw"\W+")).as("word"))
      .filter(length($"word") > 1)
      .filter(!col("word").isin(stopwords.toSeq: _*))
      .groupBy("word")
      .count()
      .orderBy(desc("count"))
      .limit(10)

    overallwords.show(false)

    spark.stop()
  }
}