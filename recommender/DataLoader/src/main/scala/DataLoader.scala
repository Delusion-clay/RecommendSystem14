import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient


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
                  timestamp: Int)

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
               timestamp: Int)

/**
 * 链接MongoDB必备配置
 *
 * @param uri 链接地址
 * @param db  库
 */
case class MongoConfig(uri: String, db: String)

/**
 * 链接ES必备配置
 *
 * @param httpHosts      主机名:9200
 * @param transportHosts 主机名:9300
 * @param index          索引库
 * @param clusterName    集群名
 */
case class ESConfig(httpHosts: String,
                    transportHosts: String,
                    index: String,
                    clusterName: String)

object DataLoader extends App {
  val MOVIE_DATA_PATH = "F:\\date\\reco_data\\small\\movies.csv"
  val RATING_DATA_PATH = "F:\\date\\reco_data\\small\\ratings.csv"
  val TAG_DATA_PATH = "F:\\date\\reco_data\\small\\tags.csv"

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"

  val ES_MOVIE_INDEX = "Movie"

  //基础配置
  val config = Map(
    "spark.cores" -> "local[*]",
    "mongo.uri" -> "mongodb://bigdata161:27017/recommender",
    "mongo.db" -> "recommender",
    "es.httpHosts" -> "bigdata161:9200",
    "es.transportHosts" -> "bigdata161:9300",
    "es.index" -> "recommender",
    "es.cluster.name" -> "my-application"
  )

  //sparkConf
  val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")

  //Spark
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  import spark.implicits._

  //三张表加载[RDD]e
  val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
  val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
  val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)

  //RDD ==> DataFrame
  val movieDF = movieRDD.map {
    item => {
      //10个字段,[(int),(9个string)]
      val attr = item.split("\\^")
      Movie(
        attr(0).toInt,
        attr(1).trim,
        attr(2).trim,
        attr(3).trim,
        attr(4).trim,
        attr(5).trim,
        attr(6).trim,
        attr(7).trim,
        attr(8).trim,
        attr(9).trim)
    }
  }.toDF()

  val ratingDF = ratingRDD.map {
    item => {
      //[Int,Int,Double,Long]
      val attr = item.split(",")
      Rating(
        attr(0).toInt,
        attr(1).toInt,
        attr(2).toDouble,
        attr(3).toInt)
    }
  }.toDF()

  val tagDF = tagRDD.map {
    item => {
      //[Int,Int,String,Long]
      val attr = item.split(",")
      Tag(
        attr(0).toInt,
        attr(1).toInt,
        attr(2).trim,
        attr(3).toInt)
    }
  }.toDF()


  movieDF.cache()
  tagDF.cache()


  //mongo配置
  implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

  //写入MongoDB
  storeDataInMongoDB(movieDF, ratingDF, tagDF)

  /**
   * 写入MongoDB中
   *
   * @param movieDF     电影表
   * @param ratingDF    评分表
   * @param tagDF       标签表
   * @param mongoConfig 基本配置
   */
  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig) = {
    //客户端
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    //有库就删除  ===> Movie | Rating | Tag
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    //写入mongoDB库
    movieDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    ratingDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    tagDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    //创建对应索引  Movie:    Rating:   Tag:
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid"->1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid"->1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid"->1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid"->1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid"->1))


    //关闭客户端
    mongoClient.close()

  }

  //如果需要功能函数,需要引入
  import org.apache.spark.sql.functions._

  //以mid为聚合,把所有的tag => tag1|tag2|tag3|xx ==> tags  ==> "mid","tags"
  val newTag = tagDF.groupBy($"mid")
      .agg(concat_ws("|",collect_set($"tag")).as("tags"))
      .select("mid","tags")

  //movie表中带有tag标签[字段]
  val movieWithTagsDF = movieDF.join(newTag,Seq("mid"),"left")

  //写入ES,eSconfig
  implicit val esConfig = ESConfig(config("es.httpHosts"),config("es.transportHosts"),config("es.index"),config("es.cluster.name"))

  //写入ES Index库
  //storeDataInES(movieWithTagsDF)

  movieDF.unpersist()
  tagDF.unpersist()

  spark.close()

  /**
   * 数据写入ES库
   *
   *
   * @param movieWithTagsDF movie表中带有tag标签[字段]
   * @param esConfig  ES基本配置
   */
  def storeDataInES(movieWithTagsDF: DataFrame)(implicit esConfig: ESConfig) = {
    //配置
    val settings = Settings.builder().put("cluster.name",esConfig.clusterName).build()

    //ES客户端
    val esClient = new PreBuiltTransportClient(settings)


    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    //集群解析  主机名1:端口号,主机名2:端口号,主机名3:端口号
    esConfig.transportHosts.split(",").foreach{
      case REGEX_HOST_PORT(host:String,port:String) => {
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host),port.toInt))
      }
    }

    //存在就删除Index
    if(esClient.admin().indices().exists(new IndicesExistsRequest(esConfig.index)).actionGet().isExists){
      esClient.admin().indices().delete(new DeleteIndexRequest(esConfig.index))
    }

    //创建索引
    esClient.admin().indices().create(new CreateIndexRequest(esConfig.index))

    //写入数据
    movieWithTagsDF
        .write
        .option("es.nodes",esConfig.httpHosts)
        .option("es.http.timeout","100m")
        .option("es.mapping.id","mid")
        .mode("overwrite")
        .format("org.elasticsearch.spark.sql")
        .save(esConfig.index + "/" + ES_MOVIE_INDEX)
  }


}
