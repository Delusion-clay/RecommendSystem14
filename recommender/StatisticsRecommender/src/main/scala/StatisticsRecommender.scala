
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

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

/**
 * [Rating.csv][电影评分表]
 * 1,
 * 1339,
 * 3.5,
 * 1260759125
 *
 * @param uid       用户ID
 * @param mid       电影ID
 * @param score     电影评分
 * @param timestamp 电影评分时间
 */
case class Rating(uid: Int,
                  mid: Int,
                  score: Double,
                  timestamp: Long)

/**
 * [Tag.csv][电影标签表]
 * 15,100365,documentary,1425876220
 *
 * @param uid       用户ID
 * @param mid       电影ID
 * @param tag       电影标签
 * @param timestamp 评分时间
 */
case class Tag(uid: Int,
               mid: Int,
               tag: String,
               timestamp: Long)

/**
 * 链接MongoDB必备配置
 *
 * @param uri 链接地址
 * @param db  库
 */
case class MongoConfig(uri: String, db: String)

case class Recommendation(mid: Int, score: Double)

/**
 * (genres,(mid,avg))
 *
 * @param genres
 * @param recs
 */
case class GenresRecommendation(genres: String, recs: Seq[Recommendation])

object StatisticsRecommender extends App {
  //从MongoDB中读取的表名
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"

  //要写入MongoDB的表名
  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"


  //基本配置
  val config = Map(
    "spark.cores" -> "local[*]",
    "mongo.uri" -> "mongodb://bigdata161:27017/recommender",
    "mongo.db" -> "recommender"
  )

  //sparkConf
  val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")

  //spark
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  //转换
  import spark.implicits._

  //mongoConfig
  implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

  //从[mongodb]中[读]取[电影表][Rating:uid|mid|score|timestamp]

  //从MongDB中读取Movie表
  val movieDF = spark.read
    .option("uri", mongoConfig.uri)
    .option("collection", MONGODB_MOVIE_COLLECTION)
    .format("com.mongodb.spark.sql")
    .load()
    .as[Movie]
    .rdd
    .toDF()

  val movieGenresDF = movieDF.map {
    case row => {
      row.getAs[String]("genres")
    }
  }.flatMap(_.split("\\|")).distinct().toDF().show()


  //从MongDB中读取Rating表
  val ratingDF = spark.read
    .option("uri", mongoConfig.uri)
    .option("collection", MONGODB_RATING_COLLECTION)
    .format("com.mongodb.spark.sql")
    .load()
    .as[Rating]
    .toDF()

  //创建临时表[视图] ==> 用于后续的sql  [uid,mid,score,timestamp]
  ratingDF.createTempView("ratings")

  //1.[评分]最多的电影 ===> count(score)  ==> group by mid
  val rateMoreMoviesDF = spark.sql("select mid, count(score) as count from ratings group by mid order by mid desc")

  //把写入mongo封装成一个方法,需要提供的参数为[DataFrame,表名]
  //storeDFInMongoDB(rateMoreMoviesDF,RATE_MORE_MOVIES)

  //2.近期热门电影  ==月  timestamp ==> yyyyMM    ratings => [uid,mid,score,timestamp]

    //DateTimeFormatter.ofPattern("yyyyMM")
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    spark.udf.register("changeDate",(x:Long) => simpleDateFormat.format(new Date(x * 1000L)))

    val ratingofYearMonth = spark.sql("select mid, score, changeDate(timestamp) as yearMonth from ratings")
    ratingofYearMonth.createTempView("ratingofYearMonth")

    val rateMoreRecentlyMovies = spark.sql("select mid, count(mid) as count, yearMonth from ratingofYearMonth group by yearMonth, mid order by yearMonth desc, count desc")
  //  storeDFInMongoDB(rateMoreRecentlyMovies,RATE_MORE_RECENTLY_MOVIES)


  //3.电影平均分  ratings => [uid,mid,score,timestamp]
  val averageMovies = spark.sql("select mid, avg(score) as avg from ratings group by mid")
  //storeDFInMongoDB(averageMovies,AVERAGE_MOVIES)


  //4.每个【类别】的【TopN】
  val movieWithScore = movieDF.join(averageMovies, "mid")

  //所有的电影的类别
  //val genres = List("Action", "Adventure", "Animation", "Comedy", "Crime", "Documentary", "Drama", "Family", "Fantasy", "Foreign", "History", "Horror", "Music", "Mystery"
  //  , "Romance", "Science", "Tv", "Thriller", "War", "Western")

  val genres = List("Crime","Romance","Thriller","Adventure","Drama","War","Documentary","Fantasy","Mystery","Musical","Animation","Film-Noir","IMAX","Horror","Western","Comedy","Children","Action","Sci-Fi")

  val genresRDD = spark.sparkContext.makeRDD(genres)

  val genresTopMoviesDF = genresRDD.cartesian(movieWithScore.rdd)
    .filter {
      case (genre, movieRow) => movieRow.getAs[String]("genres").toLowerCase.contains(genre.toLowerCase())
    }
    .map {
      //(genre,(mid,avg))
      case (genre, movieRow) => (genre, (movieRow.getAs[Int]("mid"), movieRow.getAs[Double]("avg")))
    }.groupByKey()
    .map {
      //(genre,(mid,avg))
      case (genre, items) => GenresRecommendation(genre, items.toList.sortWith(_._2 > _._2).take(10).map(item => Recommendation(item._1, item._2)))
    }.toDF()

  storeDFInMongoDB(genresTopMoviesDF, GENRES_TOP_MOVIES)

  def storeDFInMongoDB(df: DataFrame, collectionName: String)(implicit mongoConfig: MongoConfig) = {
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collectionName)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}


