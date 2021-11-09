
/**
 * 电影表
 * 1
 * Toy Story (1995)
 * *
 * 81 minutes
 * March 20, 2001
 * 1995
 * English
 * Adventure|Animation|Children|Comedy|Fantasy
 * Tom Hanks|Tim Allen|Don Rickles|Jim Varney|Wallace Shawn|John Ratzenberger|Annie Potts|John Morris|Erik von Detten|Laurie Metcalf|R. Lee Ermey|Sarah Freeman|Penn Jillette|Tom Hanks|Tim Allen|Don Rickles|Jim Varney|Wallace Shawn
 * John Lasseter
 *
 */
case class Movie(val mid: Int, val name: String, val descri: String,
                 val timelong: String, val issue: String, val shoot: String,
                 val language: String, val genres: String,
                 val actors: String, val directors: String)

/**
 * 1,31,2.5,1260759144
 * 用户对电影的评分数据集
 */
case class MovieRating(val uid: Int, val mid: Int, val score: Double, val timestamp: Int)

/**
 * 15,7478,Cambodia,1170560997
 *
 * 用户对电影的标签数据集
 */
case class Tag(val uid: Int, val mid: Int, val tag: String, val timestamp: Int)

/**
 * mongoDB 配置对象
 */
case class MongoConfig(val uri: String, val db: String)

/**
 * ES配置对象
 */
case class ESConfig(val httpHosts: String, val transportHosts: String, val index: String, val clusterName: String)

case class Recommendation(mid: Int, r: Double)

case class GenresRecommendation(genres: String, recs: Seq[Recommendation])

case class UserRecs(uid: Int, recs: Seq[Recommendation])

//电影相似度
case class MovieRecs(mid: Int, recs: Seq[Recommendation])