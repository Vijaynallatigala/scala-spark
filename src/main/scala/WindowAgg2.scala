import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class salesAgg1(spark: SparkSession) {

  import spark.implicits._

  def windowAgg1(): DataFrame = {

    val ratingData = Seq(
      ("User1", "Movie1", 4.5),
      ("User1", "Movie2", 3.5),
      ("User1", "Movie3", 2.5),
      ("User1", "Movie4", 4.0),
      ("User1", "Movie5", 3.0),
      ("User1", "Movie6", 4.5),
      ("User2", "Movie1", 3.0),
      ("User2", "Movie2", 4.0),
      ("User2", "Movie3", 4.5),
      ("User2", "Movie4", 3.5),
      ("User2", "Movie5", 4.0),
      ("User2", "Movie6", 3.5)
    )
    val ratingdf = ratingData.toDF("User", "Movie", "Rating")

    val windowspec  = Window.partitionBy("User").orderBy(desc("Movie")).rowsBetween(-2, 0)

     ratingdf.withColumn("AverageRating", avg("Rating").over(windowspec))
  }

}


object WindowAgg2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("windowagg1").master("local[*]").getOrCreate()

    val wagg1 = new salesAgg1(spark)

    val finaldf = wagg1.windowAgg1()

    finaldf.show()

  }
}