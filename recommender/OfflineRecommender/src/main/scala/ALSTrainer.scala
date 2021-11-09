import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._

object ALSTrainer {
  val MONGODB_RATING_COLLECTION = "Rating"


  def main(args: Array[String]): Unit = {
    //基础配置
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://bigdata161:27017/recommender",
      "mongo.db" -> "recommender"
    )

    //sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ALSTrainer")

    //spark
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //转换
    import spark.implicits._

    //mongConfig
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    //读取Rating
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd //用户ID,物品ID,偏好[分数]
      .map(rating => Rating(rating.uid, rating.mid, rating.score))
      .cache()

    //数据切分[2/8分]
    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
    val traningRDD = splits(0)
    val testRDD = splits(1)

    //模型训练  ===> 返回最优值
    adjustALSParam(traningRDD, testRDD)

    //关闭spark
    spark.close()

  }


  def adjustALSParam(traningRDD: RDD[Rating],
                     testRDD: RDD[Rating]) = {
    val result = for (rank <- Array(50, 60, 70, 80); lambda <- Array(0.1, 0.01, 1))
      yield { //Vector(X,X,X,X)
        val model = ALS.train(traningRDD, rank, 5, lambda)
        val rmse = getRMSE(model, testRDD)
        (rank, lambda, rmse)
      }

    //最优的参数 : rmse
    println(result.minBy(_._3))
  }

  //求两个数的均方差
  def getRMSE(model: MatrixFactorizationModel,
              data: RDD[Rating]):Double = {
    val userProducts = data.map(item => (item.user, item.product))
    val predictRating = model.predict(userProducts)

    //((user,product),rating) join ((user,product),rating)
    val a = data.map(item => ((item.user, item.product), item.rating))
    val b = predictRating.map(item => ((item.user, item.product), item.rating))

    //(uid,mid)(score1,score2)
    sqrt(
      a.join(b).map {
        case ((uid, mid), (score1, score2)) =>
          val err = score1 - score2
          err * err
      }.mean()
    )
  }
}
