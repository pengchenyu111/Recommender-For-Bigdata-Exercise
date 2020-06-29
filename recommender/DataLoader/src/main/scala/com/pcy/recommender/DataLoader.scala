package com.pcy.recommender

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author pengchenyu
 * @date 2020/6/28 18:43
 */




/**
 * 13543                                商品id
 * 蔡康永的说话之道                       商品名
 * 832,519,402                          商品分类id，不需要
 * B0047Y19CI                           亚马逊id，不需要
 * https://images-cn-4.ssl-images       商品图片URL
 * 青春文学|文学艺术|图书音像             商品分类
 * 蔡康永|写的真好|不贵|内容不错|书       用户UGC标签
 */
case class Product(
                    productId: Int,
                    name: String,
                    imageUrl: String,
                    categories: String,
                    tags: String
                  )

/**
 * 4867   评分id
 * 457976 商品id
 * 5.0    评分
 * 1395676800 时间戳
 */
case class Rating(
                   userId: Int,
                   productId: Int,
                   score: Double,
                   timestamp: Int
                 )

/**
 * MongoDB连接配置
 * @param uri 连接url
 * @param db 要操作的db
 */
case class MongoConfig(uri: String, db: String)
object DataLoader {

  //定义数据文件路径
  val PRODUCT_DATA_PATH = "D:\\MyOwnCodes\\IJIDEAJAVA\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\products.csv"
  val RATING_DATA_PATH = "D:\\MyOwnCodes\\IJIDEAJAVA\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  //定义mongodb中存储的表名
  val MONGODB_PRODUCT_COLLECTION = "Product"
  val MONGODB_RATING_COLLECTION = "Rating"

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    //创建spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    //创建spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    //加载数据
    val productRDD = spark.sparkContext.textFile(PRODUCT_DATA_PATH)
    val productDF = productRDD.map( item => {
      //product数据通过 ^ 分割
      var attr = item.split("\\^")
      //转换为Product类
      Product( attr(0).toInt, attr(1).trim, attr(4).trim, attr(5).trim, attr(6).trim)
    }).toDF()

    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = ratingRDD.map( item => {
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()

    //作为下面方法的隐式参数，免得每次都调用
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    //存入MongoDB
    storeDataInMongoDB(productDF, ratingDF)

    spark.stop()
  }

  def storeDataInMongoDB(productDF: DataFrame, ratingDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit ={
    //新建一个mongodb的连接，客户端
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    //定义要操作的表
    val productCollection = mongoClient(mongoConfig.db)(MONGODB_PRODUCT_COLLECTION)
    val ratingCollection = mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)

    //如果表存在，则删掉
    productCollection.dropCollection()
    ratingCollection.dropCollection()

    //将当前数据存入对应的表中
    productDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_PRODUCT_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //对表创建索引
    productCollection.createIndex(MongoDBObject("productId" -> 1))
    ratingCollection.createIndex(MongoDBObject("productId" -> 1))
    ratingCollection.createIndex(MongoDBObject("userId" -> 1))

    mongoClient.close()

  }
}
