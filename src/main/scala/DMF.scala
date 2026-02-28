import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{coalesce, col, count, current_date, date_format, datediff, dayofmonth, from_utc_timestamp, initcap, lit, month, sum, to_date, to_timestamp, trunc, when, year}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.immutable.Nil.groupBy

object DMF {

  def main(args: Array[String]): Unit = {

    print("helloworld")

    val spark = SparkSession.builder()
      .appName("sparkapplications")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

   1) Parsing Date and Time Strings:

     Problem: You have date and time information in string format and need to convert them into proper
     date-time objects.


   val df = List(("2023-10-07", "15:30:00")).toDF("date_str", "time_str")
  // df.printSchema()

   val formateddf = df.withColumn("date",to_date(col("date_str"))).withColumn("time_str",to_timestamp(col("time_str")))
   //formateddf.printSchema()

    formateddf.show()


   df.createOrReplaceTempView("dff")
   spark.sql(
     """
       select to_date(date_str) as date, timestamp(time_str) as time
       from dff
       """
   ).show()


   2)Date Arithmetic:

     Problem: You need to perform calculations involving date and time values, such as finding the
     difference between two dates or adding/subtracting days, months, or years.


 val df1 = List(("2023-10-07", "2023-10-10")).toDF("date1", "date2")
   val dateDiffDf = df.withColumn("days_diff", datediff($"date2", $"date1"))
   dateDiffDf.show()

   df1.createOrReplaceTempView("df2")
   spark.sql(
     """
       select date1, date2, datediff(date2, date1) as date_diff
       from df2

      """).show()

  3.  Aggregating by Date:

     Problem: You want to group data by date and perform aggregations like sum or average on groups.


   val df = List(("2023-10-07", 10), ("2023-10-07", 15), ("2023-10-08", 20)).toDF("date", "value")
   val aggregatedDf = df.groupBy(trunc($"date", "dd")).agg(sum($"value").alias("total_value"))
    aggregatedDf.show()

   df.createOrReplaceTempView("dff")
   spark.sql(
     """
       SELECT
        trunc(date, 'dd') AS day,
        SUM(value) AS total_value
        FROM dff
        GROUP BY trunc(date, 'dd')
        ORDER BY day

       """).show()


    Problem: Dealing with date and time values in different time zones and converting between them.

   val df5 = List(("2023-10-07 12:00:00", "UTC"), ("2023-10-07 12:00:00", "America/New_York"))
     .toDF("timestamp_str", "timezone")

   val convertedDf = df5.withColumn("converted_time", from_utc_timestamp($"timestamp_str", $"timezone"))
   convertedDf.show()


   df5.createOrReplaceTempView("df6")
   spark.sql(
     """
       select timestamp_str,timezone,from_utc_timestamp(timestamp_str, timezone) as converted_time
       from df6

       """).show()

   5)Handling Null or Missing Dates:

     Problem: Dealing with missing or null date values in your data.

   val df9 = Seq(("2023-10-07", null), (null, "2023-10-08")).toDF("date1", "date2")
   val filledDf = df9.withColumn("date1", coalesce($"date1", lit("2023-01-01")))
                     .withColumn("date2", when($"date2".isNull, lit("2023-12-31")).otherwise($"date2")).show()


   df9.createOrReplaceTempView("dff9")
   spark.sql(
     """
       select coalesce(date1, "2023-01-01") as date1,
       case
       when date2 is null then "2023-12-31"
       else date2
       end as date2
       from dff9

       """).show()

   Changing Date Formats:

     Use the date_format function to change the format of date and timestamp columns

   val df11 = Seq(("2023-10-07", "15:30:00")).toDF("date", "time")

   df11.createOrReplaceTempView("df12")
   val formattedDf = df11.withColumn("formatted_date", date_format($"date", "yyyy/MM/dd"))
     .withColumn("formatted_time", date_format($"time", "HH:mm:ss"))
   formattedDf.show()
   spark.sql(
     """
       select date, date_format(date,"yyyy/mm/dd") as date, time, date_format(time, "hh:mm:ss") as time
       from df12
       """).show()


   Extracting Date Components:

     You can use functions like year, month, day, hour, minute, and second to extract specific components
     from date and timestamp columns.


   val d1f = Seq(("2023-10-07 15:30:00")).toDF("timestamp")
   val extractedDf = d1f.withColumn("year", year($"timestamp"))
                        .withColumn("month", month($"timestamp"))
                        .withColumn("day", dayofmonth($"timestamp")).show()


   d1f.createOrReplaceTempView("df21")
   spark.sql(
     """
   SELECT
     year(to_timestamp(timestamp))  AS year,
     month(to_timestamp(timestamp)) AS month,
     day(to_timestamp(timestamp))   AS day
   FROM df21
 """
   ).show()





















  }

}
