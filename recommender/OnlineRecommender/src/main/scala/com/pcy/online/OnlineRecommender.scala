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
 * Windows下Flume启动：
 * bin\flume-ng.cmd agent -n agent -c conf -f conf\log-kafka.properties -property  "flume.root.logger=INFO,console"
 * 这里有点小bug：flume没有收取日志，我按照csdn上的文章改动过：conf/ flume-env.ps1  flume-ng.ps1 log-kafka.properties  bin下 flume-ng.ps1
 *
 * @author pengchenyu
 * @date 2020/6/30 10:03
 */


// 定义一个连接助手对象，建立到redis和mongodb的连接
object ConnHelper extends Serializable{
  // 懒变量定义，使用的时候才初始化
  lazy val jedis = new Jedis("localhost")
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://localhost:27017/recommender"))
}

case class MongoConfig( uri: String, db: String )

// 定义标准推荐对象
case class Recommendation( productId: Int, score: Double )
// 定义用户的推荐列表
case class UserRecs( userId: Int, recs: Seq[Recommendation] )
// 定义商品相似度列表
case class ProductRecs( productId: Int, recs: Seq[Recommendation] )

object OnlineRecommender {
  // 定义常量和表名
  val MONGODB_RATING_COLLECTION = "Rating"
  val STREAM_RECS = "StreamRecs"
  val PRODUCT_RECS = "ProductRecs"

  val MAX_USER_RATING_NUM = 20
  val MAX_SIM_PRODUCTS_NUM = 20

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )

    // 创建spark conf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OnlineRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))

    import spark.implicits._
    implicit val mongoConfig = MongoConfig( config("mongo.uri"), config("mongo.db") )

    // 加载数据，相似度矩阵，广播出去
    val simProductsMatrix = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", PRODUCT_RECS)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRecs]
      .rdd
      // 为了后续查询相似度方便，把数据转换成map形式
      .map{item =>
        ( item.productId, item.recs.map( x=>(x.productId, x.score) ).toMap )
      }
      .collectAsMap()
    // 定义广播变量
    val simProcutsMatrixBC = sc.broadcast(simProductsMatrix)

    // 创建kafka配置参数
    val kafkaParam = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )
    // 创建一个DStream
    val kafkaStream = KafkaUtils.createDirectStream[String, String]( ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String]( Array(config("kafka.topic")), kafkaParam )
    )
    // 对kafkaStream进行处理，产生评分流，userId|productId|score|timestamp
    val ratingStream = kafkaStream.map{msg=>
      var attr = msg.value().split("\\|")
      ( attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt )
    }

    // 核心算法部分，定义评分流的处理流程
    ratingStream.foreachRDD{
      rdds => rdds.foreach{
        case ( userId, productId, score, timestamp ) =>
          println("rating data coming!>>>>>>>>>>>>>>>>>>")

          // 核心算法流程
          // 1. 从redis里取出当前用户的最近评分，保存成一个数组Array[(productId, score)]
          val userRecentlyRatings = getUserRecentlyRatings( MAX_USER_RATING_NUM, userId, ConnHelper.jedis )

          // 2. 从相似度矩阵中获取当前商品最相似的商品列表，作为备选列表，保存成一个数组Array[productId]
          val candidateProducts = getTopSimProducts( MAX_SIM_PRODUCTS_NUM, productId, userId, simProcutsMatrixBC.value )

          // 3. 计算每个备选商品的推荐优先级，得到当前用户的实时推荐列表，保存成 Array[(productId, score)]
          val streamRecs = computeProductScore( candidateProducts, userRecentlyRatings, simProcutsMatrixBC.value )

          // 4. 把推荐列表保存到mongodb
          saveDataToMongoDB( userId, streamRecs )
      }
    }

    // 启动streaming
    ssc.start()
    println("streaming started!")
    ssc.awaitTermination()

  }

  /**
   * 从redis里获取最近num次评分
   */
  import scala.collection.JavaConversions._
  def getUserRecentlyRatings(num: Int, userId: Int, jedis: Jedis): Array[(Int, Double)] = {
    // 从redis中用户的评分队列里获取评分数据，list键名为uid:USERID，值格式是 PRODUCTID:SCORE
    jedis.lrange( "userId:" + userId.toString, 0, num )
      .map{ item =>
        val attr = item.split("\\:")
        ( attr(0).trim.toInt, attr(1).trim.toDouble )
      }
      .toArray
  }
  // 获取当前商品的相似列表，并过滤掉用户已经评分过的，作为备选列表
  def getTopSimProducts(num: Int,
                        productId: Int,
                        userId: Int,
                        simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                       (implicit mongoConfig: MongoConfig): Array[Int] ={
    // 从广播变量相似度矩阵中拿到当前商品的相似度列表
    val allSimProducts = simProducts(productId).toArray

    // 获得用户已经评分过的商品，过滤掉，排序输出
    val ratingCollection = ConnHelper.mongoClient( mongoConfig.db )( MONGODB_RATING_COLLECTION )
    val ratingExist = ratingCollection.find( MongoDBObject("userId"->userId) )
      .toArray
      .map{item=> // 只需要productId
        item.get("productId").toString.toInt
      }
    // 从所有的相似商品中进行过滤
    allSimProducts.filter( x => ! ratingExist.contains(x._1) )
      .sortWith(_._2 > _._2)
      .take(num)
      .map(x=>x._1)
  }
  // 计算每个备选商品的推荐得分
  def computeProductScore(candidateProducts: Array[Int],
                          userRecentlyRatings: Array[(Int, Double)],
                          simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
  : Array[(Int, Double)] ={
    // 定义一个长度可变数组ArrayBuffer，用于保存每一个备选商品的基础得分，(productId, score)
    val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    // 定义两个map，用于保存每个商品的高分和低分的计数器，productId -> count
    val increMap = scala.collection.mutable.HashMap[Int, Int]()
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()

    // 遍历每个备选商品，计算和已评分商品的相似度
    for( candidateProduct <- candidateProducts; userRecentlyRating <- userRecentlyRatings ){
      // 从相似度矩阵中获取当前备选商品和当前已评分商品间的相似度
      val simScore = getProductsSimScore( candidateProduct, userRecentlyRating._1, simProducts )
      if( simScore > 0.4 ){
        // 按照公式进行加权计算，得到基础评分
        scores += ( (candidateProduct, simScore * userRecentlyRating._2) )
        if( userRecentlyRating._2 > 3 ){
          increMap(candidateProduct) = increMap.getOrDefault(candidateProduct, 0) + 1
        } else {
          decreMap(candidateProduct) = decreMap.getOrDefault(candidateProduct, 0) + 1
        }
      }
    }

    // 根据公式计算所有的推荐优先级，首先以productId做groupby
    scores.groupBy(_._1).map{
      case (productId, scoreList) =>
        ( productId, scoreList.map(_._2).sum/scoreList.length + log(increMap.getOrDefault(productId, 1)) - log(decreMap.getOrDefault(productId, 1)) )
    }
      // 返回推荐列表，按照得分排序
      .toArray
      .sortWith(_._2>_._2)
  }

  def getProductsSimScore(product1: Int, product2: Int,
                          simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Double ={
    simProducts.get(product1) match {
      case Some(sims) => sims.get(product2) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }
  // 自定义log函数，以N为底
  def log(m: Int): Double = {
    val N = 10
    math.log(m)/math.log(N)
  }
  // 写入mongodb
  def saveDataToMongoDB(userId: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit ={
    val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(STREAM_RECS)
    // 按照userId查询并更新
    streamRecsCollection.findAndRemove( MongoDBObject( "userId" -> userId ) )
    streamRecsCollection.insert( MongoDBObject( "userId" -> userId,
      "recs" -> streamRecs.map(x=>MongoDBObject("productId"->x._1, "score"->x._2)) ) )
  }

}
