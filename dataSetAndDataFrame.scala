package examples
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}
case class SurveyRecord(Age: Int, Gender: String, Country: String, state: String)
import java.util.Properties
import scala.io.Source
object dataSetVsDataFrame extends Serializable{
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit={
    if(args.length ==0){
      logger.error("Usage: HelloDataSet filename")
      System.exit(1)
    }
  val spark = SparkSession.builder().config(getSparkAppConf).getOrCreate()
  val rawDF: Dataset[Row] = spark.read
    .option("header",value = true)
    .option("inferSchema",value = true)
    .csv(args(0))
  import spark.implicits._
  val surveyDS: Dataset[SurveyRecord] = rawDF.select("Age","Gender","Country","state").as[SurveyRecord]
  val filteredDS = surveyDS.filter(row =>row.Age < 40)
  val filtereDF = rawDF.filter("Age < 40")
    filteredDS.show()
  }
  def getSparkAppConf: SparkConf = {
    val sparkAppConf = new SparkConf()
    //Set all SparkConfigs
    val props = new Properties()
    props.load(Source.fromFile("spark.conf").bufferedReader())
    props.forEach((k, v) => sparkAppConf.set(k.toString, v.toString))
    sparkAppConf
  }
}


