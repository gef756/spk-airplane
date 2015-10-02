import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{avg, sqrt}
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

object AirplaneAnalysis {
  def stdev(col: Column): Column = {
    // Calculate standard deviation of elements in a column
    sqrt(avg(col * col) - avg(col) * avg(col))
  }

  def main(args: Array[String]) = {
    // Load data sources
    val config = ConfigFactory.load()
    val creds: Config = config.getConfig("dataSources")
    val flightFile: String = creds.getString("root") + creds.getString("flightFile")
    val carrierFile: String = creds.getString("root") + creds.getString("carrierFile")
    val planeFile: String = creds.getString("root") + creds.getString("planeFile")

    // Set up Spark Contexts
    val conf = new SparkConf().setAppName("AirplaneAnalysis")
    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)

    // Read in Data
    val csvReader: DataFrameReader = sql.read
      .format("com.databricks.spark.csv")
      .options(Map("header" -> "true",
                   "inferSchema" -> "true"))

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

    // Slice arrival delays by month
    println("---Analysis of Delays by Month---")
    val monthlyGB: GroupedData = flightData.groupBy(flightData("Month"))
    val monthlyDelays: DataFrame = monthlyGB.agg(avg(flightData("ArrDelay")),
                                                 stdev(flightData("ArrDelay")))

    monthlyDelays.foreach(println)

    // Slice arrival delays by season
    // Season 0 (Winter): Jan, Feb, Mar
    // Season 1 (Spring): Apr, May, Jun
    // Season 2 (Summer): Jul, Aug, Sep
    // Season 3 (Fall): Oct, Nov, Dec
    println("---Analysis of Delays by Season---")
    val flightDataS: DataFrame = flightData.withColumn("Season", (flightData("Month") / 4).cast(types.IntegerType))
    val seasonalDelay: DataFrame = flightDataS.groupBy(flightDataS("Season"))
      .agg(avg(flightDataS("ArrDelay")),
           stdev(flightDataS("ArrDelay")))

    seasonalDelay.foreach(println)


  }
}
