import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ConnHelper {
  //redis
  lazy val jedis = new Jedis("bigdata161")
  //mongo
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://bigdata161:27017/recommender"))
}

object StreamingRecommender {
  //从Redis中取多少个 用户的评分
  val MAX_USER_RATINGS_NUM = 10
  //相似电影候选列表
  val MAX_SIM_MOVIES_NUM = 10
  //从MovieRecs中读取表数据
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"
  //从Rating中读取表数据
  val MONGODB_RATING_COLLECTION = "Rating"
  //实时写入哪张表
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"

  def main(args: Array[String]): Unit = {
    //Kafka : Topic  => recommender   ==> Flume + Kafka [log] -> kafkaStream -> recommender

    //kafkaTopic mongo地址
    val config = Map(
      "spark.cores" -> "local[*]",
      "kafka.topic" -> "recommender",
      "mongo.uri" -> "mongodb://bigdata161:27017/recommender",
      "mongo.db" -> "recommender"
    )

    //sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StreamingRecommender")

    //Spark
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //转换
    import spark.implicits._

    //sc ssc
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))

    //kafkaPara  服务器,kv序列化,消费组
    val kafkaPara = Map(
      "bootstrap.servers" -> "bigdata161:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender"
    )


    //kafkaUtil 直连    uid|mid|score|timestamp   直连 | 手动连接 | 低阶API  ==> 手动维护offset   高阶 | 自动 ==> 自动维护offset
    val kafkaStream = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaPara))


    //sparkstreaming  ==> uid|mid|score|timestamp => uid,mid,score,timestamp
    val ratingStream = kafkaStream.map {
      case msg =>
        val attr = msg.value().split("\\|")
        (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    //读取电影相似表MovieRecs  ==> 来自离线统计的OfflineRecommender
    val simMovieMatrix = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map {
        //mid,mid,r ==> Map(mid,Map(mid,r))
        bean =>
          (bean.mid, bean.recs.map(x => (x.mid, x.r)).toMap)
      }.collectAsMap()

    //制作成广播变量
    val simMovieMatrixBroadcast = sc.broadcast(simMovieMatrix)

    ratingStream.foreachRDD {
      rdd =>
        rdd.map {
          //6|1033|5.0|949949638  ==> uid,mid,score,timestamp
          case (uid, mid, score, timestamp) =>
            println("=========开始计算==========")
            //用户最近的M个评分 ==> Redis  ==> P   Redis   12|161|3.0|835355493   A:5 B:4  A1:4.5  A2:4.8 10个
            val userRecentlyRatings = getUserRecentlyRatings(MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis)
            //找到电影P的相似电影的候选列表
            val topMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMovieMatrixBroadcast.value)
            //计算电影推荐
            val streamRecs = computeMovieScore(simMovieMatrixBroadcast.value, userRecentlyRatings, topMovies)

            //存储该值
            storeRecsInToMongoDB(uid, streamRecs)

        }.count()
    }


    ssc.start()
    ssc.awaitTermination()
  }

  def storeRecsInToMongoDB(uid: Int,
                           streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig) = {
    //存储到[StreamRecs] 中
    val streamRecsCollect = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

    //相同uid删除
    streamRecsCollect.findAndRemove(MongoDBObject("uid" -> uid))
    //插入后的格式
    streamRecsCollect.insert(MongoDBObject("uid" -> uid, "recs" -> streamRecs.map(x => x._1 + ":" + x._2).mkString("|")))

    println("save to MongoDB")
  }


  /**
   * lpush uid:6 1129:4.0 1172:4.0 1263:2.0 1287:5.0 1293:2.0 1339:3.5 1343:2.0 1371:3.5
   *
   * 返回: mid:score  电影ID:电影评分
   *
   * @param num   从redis中取出的个数
   * @param uid   用户ID
   * @param jedis 客户端
   */
  def getUserRecentlyRatings(num: Int,
                             uid: Int,
                             jedis: Jedis) = {

    jedis.lrange("uid:" + uid.toString, 0, num - 1).map {
      item =>
        val attr = item.split("\\:")

        (attr(0).trim.toInt, attr(1).trim.toDouble)
    }.toArray

  }

  /**
   *
   * @param num       电影P的相似的候选电影个数
   * @param mid       电影ID
   * @param uid       用户ID
   * @param simMovies 相似的电影的集合
   */
  def getTopSimMovies(num: Int,
                      mid: Int,
                      uid: Int,
                      simMovies: collection.Map[Int, Map[Int, Double]])(implicit mongoConfig: MongoConfig) = {
    //获取所有电影
    val allSimMovies = simMovies.get(mid).get.toArray

    //过滤掉看过的电影
    val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
      .find(MongoDBObject("uid" -> uid))
      .toArray
      .map(item => item.get("mid").toString.toInt)


    //输出
    allSimMovies.filter(x => !ratingExist.contains(x._1)).sortWith(_._2 > _._2).take(num).map(x => x._1)

  }


  /**
   *
   * @param simMovies           从广播变量中的相似电影  [Int, Map[Int, Double]]   Map(mid,Map(mid,r)) ==> MovieRecs表
   * @param userRecentlyRatings 从Redis获取的 (mid,score) 用户对mid的评分
   * @param topMovies           和电影P相似的10个电影(mid1,mid2,mid3....mid10)
   */
  def computeMovieScore(simMovies: collection.Map[Int, Map[Int, Double]],
                        userRecentlyRatings: Array[(Int, Double)],
                        topMovies: Array[Int]): Array[(Int, Double)] = {
    //保存待选电影 + 评分
    val score = ArrayBuffer[(Int, Double)]()
    //增强因子
    val increMap = mutable.HashMap[Int, Int]()
    //减弱因子
    val decreMap = mutable.HashMap[Int, Int]()

    for (topMovie <- topMovies; userRecentlyRating <- userRecentlyRatings) {
      val simScore = getMoviesSimScore(simMovies, userRecentlyRating._1, topMovie)

      if (simScore > 0.6) {
        score += ((topMovie, simScore * userRecentlyRating._2))
        if (userRecentlyRating._2 > 3) {
          //增强因子
          increMap(topMovie) = increMap.getOrDefault(topMovie, 0) + 1
        } else {
          //减弱因子
          decreMap(topMovie) = decreMap.getOrDefault(topMovie, 0) + 1
        }
      }
    }

    score.groupBy(_._1).map {
      case (mid, sims) =>
        (mid, sims.map(_._2).sum / sims.length + log(increMap(mid)) - log(decreMap(mid)))
    }.toArray

  }

  def log(i: Int) = {
    math.log(i) / math.log(2)
  }


  /**
   *
   * @param simMovies       从广播变量中的相似电影  [Int, Map[Int, Double]]   Map(mid,Map(mid,r)) ==> MovieRecs表
   * @param userRatingMovie 从Redis获取的 (mid,score) 用户对mid的评分
   * @param topMovie        和电影P相似的10个电影(mid1,mid2,mid3....mid10)
   */
  def getMoviesSimScore(simMovies: collection.Map[Int, Map[Int, Double]],
                        userRatingMovie: Int,
                        topMovie: Int) = {

    simMovies.get(topMovie) match {
      case Some(x) => x.get(userRatingMovie) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }
}
