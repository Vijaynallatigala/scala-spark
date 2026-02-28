import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object leadandlag {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DateDiffExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    import java.sql.Date
    spark.sparkContext.setLogLevel("ERROR")

    print("scala spark")
//
//    val data = Seq(
//      (1, "KitKat", 1000, Date.valueOf("2021-01-01")),
//      (1, "KitKat", 2000, Date.valueOf("2021-01-02")),
//      (1, "KitKat", 1000, Date.valueOf("2021-01-03")),
//      (1, "KitKat", 2000, Date.valueOf("2021-01-04")),
//      (1, "KitKat", 3000, Date.valueOf("2021-01-05")),
//      (1, "KitKat", 1000, Date.valueOf("2021-01-06"))
//    )
//
//    val df = data.toDF("IT_ID", "IT_Name", "Price", "PriceDate")
//
//    df.show()
//
//
//    // we want to find the difference between the price on each day with its previous day
//
//
//    print("problems")
//    print("scala spark")
//    val windowspec = Window.partitionBy("IT_ID", "IT_Name").orderBy("PriceDate")
//
//    val resultdf= df.withColumn("prev_Price", lag("price", 1).over(windowspec))
//                    .withColumn("price_diff", col("Price") - col("prev_price"))
//
//    resultdf.show()
//
//    print("sql spark")
//    df.createOrReplaceTempView("dff")
//    spark.sql(
//      """
//      select *,
//      lag(Price, 1) over(partition by IT_ID, IT_Name order by PriceDate) as lagg,
//      Price - Lag(Price,1) over(partition by IT_ID, IT_Name order by PriceDate) as price_diff
//      from dff
//        """).show()

    val data = Seq(
      (1, "John", 1000, "01/01/2016"),
      (1, "John", 2000, "02/01/2016"),
      (1, "John", 1000, "03/01/2016"),
      (1, "John", 2000, "04/01/2016"),
      (1, "John", 3000, "05/01/2016"),
      (1, "John", 1000, "06/01/2016")
    )

    val df = data.toDF("ID", "NAME", "SALARY", "DATE")
                 .withColumn("DATE", to_date(col("DATE"), "dd/MM/yyyy"))

    // if salary is less than previous month we will mark it as "Down", if salary has increased then "UP"

    val window = Window.partitionBy("ID", "NAME").orderBy("DATE")

    val result = df
      .withColumn("prev_salary", lag("SALARY", 1).over(window))
      .withColumn(
        "STATUS",
        when(col("prev_salary").isNull, null)
          .when(col("SALARY") > col("prev_salary"), "UP")
          .when(col("SALARY") < col("prev_salary"), "DOWN")
          .otherwise("SAME")
      )
    result.show()

    df.createOrReplaceTempView("dff")
    spark.sql(
      """
  WITH salary_lag AS
  (
      SELECT
          ID,
          NAME,
          SALARY,
          DATE,
          LAG(SALARY, 1) OVER (PARTITION BY ID, NAME ORDER BY DATE) AS prev_salary
      FROM dff)
  SELECT
      ID,
      NAME,
      SALARY,
      DATE,
      prev_salary,
      CASE
          WHEN prev_salary IS NULL THEN NULL
          WHEN SALARY > prev_salary THEN 'UP'
          WHEN SALARY < prev_salary THEN 'DOWN'
          ELSE 'SAME'
      END AS STATUS
  FROM salary_lag
    """
).show()




  }
  }
