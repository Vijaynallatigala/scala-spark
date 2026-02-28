import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class SalesAnalytics(spark: SparkSession) {

  import spark.implicits._

  def calculateRunningTotal(): DataFrame = {

    val salesData = Seq(
      ("Product1", "Category1", 100),
      ("Product2", "Category2", 200),
      ("Product3", "Category1", 150),
      ("Product4", "Category3", 300),
      ("Product5", "Category2", 250),
      ("Product6", "Category3", 180)
    )

    val salesDF = salesData.toDF("Product", "Category", "Revenue")

    val windowSpec = Window
      .partitionBy("Category")
      .orderBy("Product")

    salesDF.withColumn(
      "Running_total",
      sum("Revenue").over(windowSpec)
    )
  }
}


// Main Object (Entry Point)
object WindowAggregations {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Sales Running Total")
      .master("local[*]")
      .getOrCreate()

    val analytics = new SalesAnalytics(spark)

    val finalDF = analytics.calculateRunningTotal()

    finalDF.show()

  }
}

