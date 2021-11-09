import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

// 需要rating数据
case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Long)

case class MongoConfig(uri: String, db: String)

/**
 * [Movie.csv][电影表]
 * 13^
 * Balto (1995)^
 * ^
 * 78 minutes^
 * February 19, 2002^
 * 1995^
 * English ^
 * Adventure|Animation|Children ^
 * Kevin Bacon|Bob Hoskins|Bridget Fonda|Jim Cummings|Phil Collins|Juliette Brewer|Jack Angel|Danny Mann|Robbie Rist|Sandra Dickinson|Frank Welker|Miriam Margolyes|Lola Bates-Campbell|Kevin Bacon|Bob Hoskins|Bridget Fonda|Jim Cummings|Phil Collins ^
 * Simon Wells
 *
 *
 * @param mid       电影ID
 * @param name      电影名称
 * @param descri    电影描述
 * @param timelong  电影时长
 * @param shoot     拍摄时间
 * @param issue     发行时间
 * @param language  电影语言
 * @param genres    电影类别
 * @param actors    电影演员
 * @param directors 电影导演
 */
case class Movie(mid: Int,
                 name: String,
                 descri: String,
                 timelong: String,
                 shoot: String,
                 issue: String,
                 language: String,
                 genres: String,
                 actors: String,
                 directors: String)

// 推荐
case class Recommendation(mid: Int, score: Double)

// 用户推荐列表
case class UserRecs(uid: Int, recs: Seq[Recommendation])

// 【物品】电影相似度列表
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

/**
 * 用户ID【uid】,物品ID【mid】,偏好值【score】  ==> MovieRating
 */
object OfflineRecommender {

  // 从Rating中取数据
  val MONGODB_RATING_COLLECTION = "Rating"

  val MONGODB_MOVIE_COLLECTION="Movie"

  //要做的，写出的【MongoDB】表
  val USER_RECS = "UserRecs"
  val MOVIE_RECS = "MovieRecs"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://bigdata161:27017/recommender",
      "mongo.db" -> "recommender"
    )

    //sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")

    //spark
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //转换
    import spark.implicits._

    //mongoConfig
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))


    //读取Rating表中的数据
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => (rating.uid, rating.mid, rating.score))
      .cache()

    //得到用户ID和物品ID【movieID】
    val userRDD = ratingRDD.map(_._1).distinct()
    //val movieRDD = ratingRDD.map(_._2).distinct() //mid ==> Rating的mid字段

    val movieRDD = spark
      .read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .rdd
      .map(_.mid).cache


    //uid, mid, score[ratingRDD][trainData]    (user, product, rating)
    val trainData = ratingRDD.map(x => Rating(x._1, x._2, x._3))

    //ALS  训练集[Rating] , 特征值[50] , 迭代参数[5] , 防过拟合参数[0.1]
    val (rank, iterations, lamda) = (50, 5, 0.1)

    val model = ALS.train(trainData, rank, iterations, lamda)
    //用户ID和电影ID的矩阵  [UID * MID]
    val userMovies = userRDD.cartesian(movieRDD)

    val preRatings = model.predict(userMovies)
    val userRecs = preRatings
      .filter(_.rating > 0)
      //(user, (product, rating))
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map {
        //(genres,(mid,score))  ===> (uid,(mid,score))
        case (uid, recs) => UserRecs(uid, recs.toList.sortWith(_._2 > _._2).take(10).map(x => Recommendation(x._1, x._2)))
      }.toDF()

    //用户推荐  sortWith(_._2 > _._2)   ===> orderby .take(10)  ===> limit 10
    userRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    val movieFeatures = model.productFeatures.map {
      //(电影id,产品特征)
      case (mid, features) => (mid, new DoubleMatrix(features))
    }

    val movieRecs = movieFeatures.cartesian(movieFeatures)
      .filter {
        //过滤自己，自己和自己不能相似  a是一个电影  b是另一个电影
        case (a, b) => a._1 != b._1
      }
      .map {
        case (a, b) => {
          //两个产品之间相似分数
          val simScore = this.consinSim(a._2, b._2)
          //(a,(b,simScore))
          (a._1, (b._1, simScore))
        }
      }.filter(_._2._2 > 0.8) //相似度大于0.6
        .groupByKey()
        .map{
          case (mid, recs) => MovieRecs(mid, recs.toList.map(x => Recommendation(x._1, x._2)))
        }.toDF()

    movieRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    spark.close()

  }

  /**
   * 计算两个电影的相似度 返回Double
   * @param movie1 a电影
   * @param movie2 b电影
   * @return 返回Double分数
   */
  def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix) = {
    //两个电影的点承 / 两电影的范数
    movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
  }


}
