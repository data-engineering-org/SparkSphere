import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{explode, regexp_replace, row_number, split, col, to_date, date_format}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types.{IntegerType, StringType,StructType,StructField}
object sampleTest {
  def main(args : Array[String]): Unit = {
    Logger.getLogger(getClass.getName).info("My Application is Starting")
    val sparkConf =  new SparkConf()
    val schema = StructType(List(StructField("id", IntegerType,true),
    StructField("name", StringType,true),
    StructField("age", IntegerType,true)
    ))
    sparkConf.set("spark.master","local[4]")
    sparkConf.set("spark.app.name","movieGenre")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val bookingDF = spark.read.option("header",value = true).option("delimiter",value  = " | ").option("inferSchema",value = true).csv("C:/Users/Admin/Downloads/spark/inputDir/movieBooking.csv")
    val genreDF = spark.read.option("header",value = true).option("delimiter",value  = " | ").option("inferSchema",value = true).csv("C:/Users/Admin/Downloads/spark/inputDir/genre.csv")
    val genreDetail = genreDF.withColumnRenamed("movieId","genreMovieId")
    val bookingNov2023 = bookingDF.select(date_format(to_date(col("date"), "yyyy-MM-dd"), "yyyyMM").as("billDate"),col("Movies"))
                  .filter("billDate = 202311")
    val movieList = bookingNov2023.withColumn("Movies", split(regexp_replace(col("Movies"), "\\[|\\]", ""), ",\\s*")).select(explode(col("Movies")).alias("movieId"))
    val movieCount = movieList.groupBy("movieId").count()
    val movieAndGenre =  movieCount.join(genreDetail,movieCount.col("movieId") === genreDetail.col("genreMovieId"),"inner")
    movieAndGenre.show()
     val windowSpec = Window.partitionBy(col("Genre")).orderBy(col("count").desc)
    val top10MoviePerGenre = movieAndGenre.withColumn("rnm",row_number() over(windowSpec))
    top10MoviePerGenre.filter("rnm <= 3").select("movieId","Genre","rnm").show()
  }
}
