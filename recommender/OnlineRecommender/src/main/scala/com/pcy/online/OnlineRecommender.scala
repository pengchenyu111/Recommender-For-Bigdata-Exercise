package com.pcy.online

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 *
 * windows下Kafka启动：
 * .\bin\windows\zookeeper-server-start.bat  .\config\zookeeper.properties
 * .\bin\windows\kafka-server-start.bat .\config\server.properties
 * 创建topic：
 * .\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic recommender
 * 创建生产者：
 * .\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic recommender
 *
 * Windows下redis启动：
 * redis-server.exe redis.windows.conf
 *
 * @author pengchenyu
 * @date 2020/6/30 10:03
 */


// 连接助手对象
object ConnHelper extends Serializable{
  lazy val jedis = new Jedis("localhost")
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://localhost:27017/recommender"))
}

case class MongoConfig(uri:String,db:String)

// 标准推荐
case class Recommendation(productId:Int, score:Double)
// 用户的推荐
case class UserRecs(userId:Int, recs:Seq[Recommendation])
//商品的相似度
case class ProductRecs(productId:Int, recs:Seq[Recommendation])


object OnlineRecommender {

  //定义表和常量
  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_PRODUCTS_NUM = 20

  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_PRODUCT_RECS_COLLECTION = "ProductRecs"

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )

    //创建一个SparkConf配置
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OnlineRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))

    implicit val mongConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    import spark.implicits._

    //加载数据， 广播商品相似度矩阵,  装换成为 Map[Int, Map[Int,Double]]
    val simProductsMatrix = spark
      .read
      .option("uri",config("mongo.uri"))
      .option("collection",MONGODB_PRODUCT_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRecs]
      .rdd
      .map{recs =>
        (recs.productId,recs.recs.map(x=> (x.productId,x.score)).toMap)
      }.collectAsMap()

    // 定义广播变量
    val simProductsMatrixBroadCast = sc.broadcast(simProductsMatrix)

    //创建到Kafka的连接
    val kafkaPara = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    // 创建DStream
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(config("kafka.topic")), kafkaPara))

    // 对kafkaStream预处理，产生评分流： UID|MID|SCORE|TIMESTAMP
    val ratingStream = kafkaStream.map{
      case msg=>
      var attr = msg.value().split("\\|")
      (attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
    }

    // 核心实时推荐算法， 定义评分流的处理流程
    ratingStream.foreachRDD{
      rdd => rdd.foreach{
        case (userId, productId, score, timestamp) =>
        println("rating data coming >>>>>>>>>>>>>>>>")

        //从 redis 中获取当前最近的M次商品评分，保存为数组Array[(productId, score)]
        val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATINGS_NUM, userId, ConnHelper.jedis)

        //从相似度矩阵中获取商品P最相似的K个商品，作为备选商品列表,保存为数组Array[productId]
        val simProducts = getTopSimProducts(MAX_SIM_PRODUCTS_NUM, productId, userId, simProductsMatrixBroadCast.value)

        //计算每个备选商品的推荐优先级，得到当前用户的实时推荐列表
        val streamRecs = computeProductScores(simProductsMatrixBroadCast.value,userRecentlyRatings,simProducts)

        //将推荐列表保存到MongoDB
        saveRecsToMongoDB(userId,streamRecs)

      }
    }

    //启动Streaming程序
    ssc.start()
    printf("streaming start >>>>>>>>")
    ssc.awaitTermination()
  }

  import scala.collection.JavaConversions._
  /**
   * 获取当前最近的M次商品评分
   * @param num 评分的个数
   * @param userId 谁的评分
   * @param jedis
   * @return
   */
  def getUserRecentlyRating(num:Int, userId:Int,jedis:Jedis): Array[(Int,Double)] ={
    //从用户的队列中取出num个评分
    jedis.lrange("userId:"+userId.toString, 0, num)
      .map{item =>
      val attr = item.split("\\:")
      (attr(0).trim.toInt, attr(1).trim.toDouble)
    }.toArray
  }


  /**
   * 获取当前商品K个相似的商品
   * @param num          相似商品的数量
   * @param productId    当前商品的ID
   * @param userId       当前的评分用户
   * @param simProducts  商品相似度矩阵的广播变量值
   * @param mongoConfig   MongoDB的配置
   * @return
   */
  def getTopSimProducts(num:Int,
                        productId:Int,
                        userId:Int,
                        simProducts:scala.collection.Map[Int, scala.collection.immutable.Map[Int,Double]])(implicit mongoConfig: MongoConfig): Array[Int] ={
    //从广播变量的商品相似度矩阵中获取当前商品所有的相似商品
    val allSimProducts = simProducts(productId).toArray
    //获取用户已经评分过得商品，过滤掉，排序输出
    val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
      .find(MongoDBObject("userId" -> userId))
      .toArray
      .map{item =>
      item.get("productId").toString.toInt
    }
    //过滤掉已经评分过得商品，并排序输出
    allSimProducts.filter(x => !ratingExist.contains(x._1))
      .sortWith(_._2 > _._2)
      .take(num)
      .map(x => x._1)
  }

  /**
   * 计算待选商品的推荐分数
   * @param simProducts            商品相似度矩阵
   * @param userRecentlyRatings  用户最近的k次评分
   * @param topSimProducts         当前商品最相似的K个商品
   * @return
   */
  def computeProductScores( simProducts:scala.collection.Map[Int,scala.collection.immutable.Map[Int,Double]],
                            userRecentlyRatings:Array[(Int,Double)],
                            topSimProducts: Array[Int]): Array[(Int,Double)] ={

    //用于保存每一个待选商品和最近评分的每一个商品的权重得分，（productId, score）
    val score = scala.collection.mutable.ArrayBuffer[(Int,Double)]()
    //用于保存每一个商品的增强因子数
    val increMap = scala.collection.mutable.HashMap[Int,Int]()
    //用于保存每一个商品的减弱因子数
    val decreMap = scala.collection.mutable.HashMap[Int,Int]()

    //遍历每个备选商品，计算和已评分商品的相似度
    for (topSimProduct <- topSimProducts; userRecentlyRating <- userRecentlyRatings){
      //从相似度矩阵中获取当前备选商品和当前已评分商品间的相似度
      val simScore = getProductsSimScore(simProducts,userRecentlyRating._1,topSimProduct)
      if(simScore > 0.6){
        // 按照公式计算
        //得到基础评分
        score += ((topSimProduct, simScore * userRecentlyRating._2 ))
        if(userRecentlyRating._2 > 3){
          // 增强因子
          increMap(topSimProduct) = increMap.getOrDefault(topSimProduct,0) + 1
        }else{
          // 减弱因子
          decreMap(topSimProduct) = decreMap.getOrDefault(topSimProduct,0) + 1
        }
      }
    }

    // 根据公式计算所有的推荐优先级，首先以productId 做group by
    score.groupBy(_._1).map{
      case (productId, scoreList) =>
      (productId, scoreList.map(_._2).sum / scoreList.length + log(increMap.getOrDefault(productId, 1)) - log(decreMap.getOrDefault(productId, 1)))
    }.toArray.sortWith(_._2 >_._2)

  }

  /**
   * 获取当个商品之间的相似度
   * @param simProducts       商品相似度矩阵
   * @param userRatingProduct 用户已经评分的商品
   * @param topSimProduct     候选商品
   * @return
   */
  def getProductsSimScore( simProducts:scala.collection.Map[Int,scala.collection.immutable.Map[Int,Double]],
                           userRatingProduct:Int, topSimProduct:Int): Double ={
    simProducts.get(topSimProduct) match {
      case Some(sim) => sim.get(userRatingProduct) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  /**
   * 取10的对数
   * @param m
   * @return
   */
  def log(m:Int):Double ={
    math.log(m) / math.log(10)
  }

  /**
   * 将数据保存到MongoDB    userId -> 1,  recs -> 22:4.5|45:3.8
   * @param streamRecs  流式的推荐结果
   * @param mongoConfig  MongoDB的配置
   */
  def saveRecsToMongoDB(userId:Int, streamRecs:Array[(Int,Double)])(implicit mongoConfig: MongoConfig): Unit ={
    //到StreamRecs的连接
    val streaRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

    streaRecsCollection.findAndRemove(MongoDBObject("userId" -> userId))
    streaRecsCollection.insert(MongoDBObject("userId" -> userId, "recs" ->
      streamRecs.map( x => MongoDBObject("productId"->x._1, "score"->x._2)) ))
  }







}
