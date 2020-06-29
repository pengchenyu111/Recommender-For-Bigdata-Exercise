package com.pcy.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author pengchenyu
 * @date 2020/6/29 10:48
 */



case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)

case class MongoConfig(uri: String, db: String)


object StatisticsRecommender {

  val MONGODB_RATING_COLLECTION = "Rating"

  //定义mongodb中统计后的存储表
  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  val RATE_RECENTLY_PRODUCTS = "RateRecentlyProducts"
  val AVERAGE_PRODUCTS = "AverageProducts"

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    //创建spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")
    //创建spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    //加载数据
    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    //创建rating临时表
    ratingDF.createOrReplaceTempView("ratings")

    //1.历史热门数据，按评分个数
    val rateMoreProductsDF = spark.sql("select productId, count(productId) as count from ratings group by productId order by count desc")
    storeDFInMongoDB(rateMoreProductsDF, RATE_MORE_PRODUCTS)


    //2.近期热门数据，吧时间戳转换为yyyyMM格式进行评分个数统计
    //创建日期的格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    //注册UDF，将timestamp转化为年月格式yyyyMM,乘1000是因为提供的数据单位是秒，Date要的是毫米的long
    spark.udf.register("changeDate", (x : Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)
    //把原始rating数据转化为想要的结构：productId, score, yearMonth
    val ratingOfYearMonthDF = spark.sql("select productId, score, changeDate(timestamp) as yearMonth from ratings")
    ratingOfYearMonthDF.createOrReplaceTempView("ratingOfMonth")
    val rateMoreRecentlyProductsDF = spark.sql("select productId, count(productId) as count, yearMonth from ratingOfMonth group by yearMonth, productId order by yearMonth desc, count desc")
    storeDFInMongoDB(rateMoreRecentlyProductsDF, RATE_RECENTLY_PRODUCTS)


    //3.优质商品统计，商品的平均评分
    val averageProducts = spark.sql("select productId, avg(score) as avg from ratings group by productId order by avg desc")
    storeDFInMongoDB( averageProducts, AVERAGE_PRODUCTS)

    spark.stop()
  }

  def storeDFInMongoDB(df: DataFrame, collectionName: String)(implicit mongoConfig: MongoConfig): Unit ={
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collectionName)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


  }
}
