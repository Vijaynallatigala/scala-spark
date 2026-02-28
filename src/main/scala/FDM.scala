import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FDM {

  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder()
      .appName("DateDiffExample")
      .master("local[*]")
      .getOrCreate()

    //Calculate the number of days between two dates using Spark SQL.

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")


//    print("scala Spark")
//
//    val df = List(("2023-10-07", "2023-10-10")).toDF("date1", "date2")
//
//    val dateDiffDf = df.withColumn("days_diff", datediff($"date2", $"date1"))
//    dateDiffDf.show()
//
//    print("spark sql")
//
//    df.createOrReplaceTempView("df1")
//    spark.sql(
//      """
//        select date1, date2, datediff(date2, date1) as date_diff
//        from df1
//        """).show()


    print("scala Spark")

    // Given a DataFrame with date and value columns, calculate the average value for each month

//    val df = List(("2023-10-07", 10), ("2023-10-07", 15), ("2023-11-08", 20)).toDF("date", "value")
//
//    val df1 = df.withColumn("year_month", date_format(col("date"),"yyyy-mm")).groupBy(col("year_month")).agg(avg("value"))
//    df1.show()
//
//    print("Spark sql")
//
//    df.createOrReplaceTempView("df1")
//    spark.sql(
//      """
//            SELECT
//            year(date) as year_date,
//            month(date) as month_date,
//            avg(value) as avg_values
//            from df1
//            group by  year_date,month_date
//
//        """).show()


//    Given a DataFrame with timestamp and timezone columns, convert all timestamps to a target time zone

//    val df = List(("2023-10-07 12:00:00", "UTC"), ("2023-10-07 12:00:00","America/New_York")).toDF("timestamp_str", "timezone")
//
//
//    val convertedDf = df.withColumn("converted_time", from_utc_timestamp($"timestamp_str", $"timezone"))

//    Given a DataFrame with date1 and date2 columns, handle missing date values by filling them with default dates.

//    val df = List(("2023-10-07", null), (null, "2023-10-08")).toDF("date1", "date2")
//
//    val filledDf = df.withColumn("date1", coalesce($"date1", lit("2023-01-01")))
//                     .withColumn("date2", coalesce($"date2",lit("2023-01-01")))
//                     //.withColumn("date2", when($"date2".isNull, lit("2023-12-31")).otherwise($"date2"))
//    filledDf.show()
//
//    df.createOrReplaceTempView("dff")
//    spark.sql(
//      """
//        select date1, date2,
//        coalesce(date1,  DATE '2023-01-01') as datee,
//        case
//          when date2 is null then DATE "2023-12-31"
//          else date2
//          end as date22
//        from dff
//        """).show()

//    Given a DataFrame with a timestamp column, extract the day of the week for
//      each timestamp and display it as a new column.

//    val df = List("2023-10-07 12:00:00", "2023-10-10 15:30:00").toDF("timestamp_str")
//
//    val dayOfWeekDf = df.withColumn("timestamp", to_timestamp($"timestamp_str")).withColumn("day_of_week", date_format($"timestamp", "EEEE"))
//    dayOfWeekDf.show()
//
//    df.createOrReplaceTempView("df")
//
//    spark.sql("""
//  SELECT
//    timestamp_str,
//    to_timestamp(timestamp_str) AS timestamp,
//    date_format(to_timestamp(timestamp_str), 'EEEE') AS day_of_week
//  FROM df
//""").show()

//    Given a DataFrame with a date column, find the maximum and minimum
//    dates in the dataset



    val df = List("2023-10-07", "2023-10-10", "2023-10-01").toDF("date_str")
    df.show()

    val maxMinDateDf = df.withColumn("date", to_date($"date_str"))

    val maxDate = maxMinDateDf.select(max($"date")).collect()(0)(0)

    val minDate = maxMinDateDf.select(min($"date")).collect()(0)(0)
    println(s"Maximum Date: $maxDate")
    println(s"Minimum Date: $minDate")


  }

}
