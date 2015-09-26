import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrameReader, DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

object AirplaneAnalysis {
  def main(args: Array[String]) = {
    val config = ConfigFactory.load()
    val creds: Config = config.getConfig("dataSources")
    val flightFile: String = creds.getString("root") + creds.getString("flightFile")
    val carrierFile: String = creds.getString("root") + creds.getString("carrierFile")
    val planeFile: String = creds.getString("root") + creds.getString("planeFile")


    val conf = new SparkConf().setAppName("AirplaneAnalysis")
    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)

    val csvReader: DataFrameReader = sql.read
      .format("com.databricks.spark.csv")
      .options(Map("header" -> "true"))

    val flightData: DataFrame = csvReader
      .load(flightFile)
    println("---Flight data loaded.---")
    flightData.printSchema()

    val carrierData: DataFrame = csvReader
      .load(carrierFile)
    println("---Carriers have arrived.---")
    carrierData.printSchema()

    val planeData: DataFrame = csvReader
      .load(planeFile)
    println("---Plane data loaded.---")
    planeData.printSchema()


  }
}
