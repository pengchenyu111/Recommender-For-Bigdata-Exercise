package com.pcy.offline

import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * 结果为：
 * (250,0.001,1.3314707376482553)
 * @author pengchenyu
 * @date 2020/6/29 15:24
 */


object ALSTrainer {

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    //创建SparkConf
    val sparkConf = new SparkConf().setAppName("ALSTrainer").setMaster(config("spark.cores"))
    //创建SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    import spark.implicits._

    //加载评分数据
    val ratingRDD = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", OfflineRecommender.MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd
      .map(rating => Rating(rating.userId, rating.productId, rating.score)).cache()


    // 将一个RDD随机切分成两个RDD，用以划分训练集和测试集
    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
    val trainingRDD = splits(0)
    val testingRDD = splits(1)

    //输出最优参数
    adjustALSParams(trainingRDD, testingRDD)

    //关闭Spark
    spark.close()
  }


  // 输出最终的最优参数
  def adjustALSParams(trainData:RDD[Rating], testData:RDD[Rating]): Unit ={
    // 这里指定迭代次数为5，rank和lambda在几个值中选取调整
    val result = for(rank <- Array(100, 200, 250); lambda <- Array(1, 0.1, 0.01, 0.001))
      yield {
        val model = ALS.train(trainData, rank, 5, lambda)
        val rmse = getRMSE(model, testData)
        (rank,lambda,rmse)
      }
    // 按照rmse排序输出最优参数
    println(result.sortBy(_._3).head)
  }

  def getRMSE(model:MatrixFactorizationModel, data:RDD[Rating]):Double={
    // 得到预测评分矩阵
    val userProducts = data.map(item => (item.user, item.product))
    val predictRating = model.predict(userProducts)

    //按照公式计算rmse，首先把预测评分和实际评分表按照（userId, productId）做一个连接
    val real = data.map(item => ((item.user, item.product), item.rating))
    val predict = predictRating.map(item => ((item.user, item.product), item.rating))
    // 计算RMSE
    sqrt(
      real.join(predict).map{case ((userId, productId), (real, pre))=>
        // 真实值和预测值之间的差
        val err = real - pre
        err * err
      }.mean() //mean算均值
    )
  }




}
