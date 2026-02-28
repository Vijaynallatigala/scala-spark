import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object rank_denserank_problems {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DateDiffExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    print("scala spark")

    val data = Seq(
      ("Karthik", "Sales", 2023, 1200),
      ("Ajay", "Marketing", 2022, 2000),
      ("Vijay", "Sales", 2023, 1500),
      ("Mohan", "Marketing", 2022, 1500),
      ("Veer", "Sales", 2021, 2500),
      ("Ajay", "Finance", 2023, 1800),
      ("Kiran", "Sales", 2023, 1200),
      ("Priya", "Finance", 2023, 2200),
      ("Karthik", "Sales", 2022, 1300),
      ("Ajay", "Marketing", 2023, 2100),
      ("Vijay", "Finance", 2022, 2100),
      ("Kiran", "Marketing", 2023, 2400),
      ("Mohan", "Sales", 2022, 1000)
    ).toDF("Name", "Department", "Year", "Salary")

    //1. For each department, assign ranks to employees based on their salary in descending order
    //for each year

//    val windowspec = Window.partitionBy("Department", "Year").orderBy(col("Salary").desc)
//
//    val resultdf = data.withColumn("rank", rank().over(windowspec))
//
//    resultdf.show()
//
//    print("spark sql")
//
//    data.createOrReplaceTempView("datadf")
//    spark.sql(
//      """
//        select *,
//        rank() over(partition by Department, Year order by Salary desc) as rnk
//        from datadf
//        """).show()

    // In the "Sales" department, rank employees based on their salary. If two employees have the
    //same salary, they should share the same rank.

//    val windowspec = Window.partitionBy("Department").orderBy(col("Salary").desc)
//    val resultdf = data.withColumn("rank", rank() over(windowspec))
//    resultdf.show()
//
//    print("spark sql")
//    data.createOrReplaceTempView("datadf")
//    spark.sql(
//      """
//        select *,
//        rank() over(partition by Department order by Salary desc) as rnk
//        from datadf
//        """).show()

//    For each department, assign a row number to each employee based on the year, ordered by
//      salary in descending order.

//    val windowspec = Window.partitionBy("Department").orderBy(col("Salary").desc)
//
//    val resultdf = data.withColumn("row_number", row_number() over(windowspec))
//    resultdf.show()
//
//    print("scala sql")
//
//    data.createOrReplaceTempView("datadf")
//    spark.sql(
//      """
//        select *,
//        row_number() over(partition by Department order by Salary desc) as rwn
//        from datadf
//        """).show()

    //Rank employees across all departments based on their salary within each year.

//    val windowspec = Window.partitionBy("Year") orderBy(col("Salary").desc)
//
//    val resultdf = data.withColumn("Rank", rank() over(windowspec))
//    resultdf.show()
//
//    print("spark sql")
//    data.createOrReplaceTempView("datadf")
//    spark.sql(
//      """
//        select *,
//        row_number() over(partition by Year order by Salary desc) as rwn
//        from datadf
//        """).show()

//    Rank employees by their salary in descending order. If multiple employees have the same
//      salary, ensure they receive unique rankings without gaps.

//    val windowspec = Window.orderBy(col(("Salary")).desc)
//    val resultdf = data.withColumn("row_number", row_number() over(windowspec))
//    resultdf.show()
//
//    print("Spark sql")
//
//    data.createOrReplaceTempView("datadf")
//    spark.sql(
//      """
//        select *,
//        row_number() over(order by Salary desc) as rwn
//        from datadf
//        """).show()

//    In each department, rank employees based on their salary. Show ranks as consecutive
//      numbers even if salaries are tied.

//    val windowspec = Window.partitionBy("Department").orderBy(col("Salary").desc)
//
//    val resultdf = data.withColumn("rank", dense_rank() over(windowspec))
//    resultdf.show()
//
//    print("spark sql")
//
//    data.createOrReplaceTempView("datadf")
//    spark.sql(
//      """
//        select *,
//        dense_rank() over(partition by Department order by Salary desc) as drk
//        from datadf
//        """).show()


    // List employees with the top 2 highest salaries in the "Finance" department for each year.

//
//    val financeDf = data.filter(col("Department")==="Finance")
//    val windowspec = Window.partitionBy("Year").orderBy(col("Salary").desc)
//
//    val resultdf = financeDf. withColumn("drank", dense_rank() over(windowspec)).filter(col("drank") <= 2)
//    resultdf.show()
//
//
//    print("spark sql")
//
//    data.createOrReplaceTempView("datadf")
//    spark.sql(
//      """
//        with cte as (
//        select *,
//        dense_rank() over(partition by Year order by Salary desc) as drk
//        from datadf
//        where Department = "Finance"
//      )
//      Select *
//      from cte
//      where drk <=2
//        """).show()


//    For each department, rank employees based on their salary within each year. Display only
//      employees ranked in the top 3 for each department and year.

    val windowspec = Window.partitionBy("Department", "Year").orderBy(col("Salary").desc)
    val resultdf = data.withColumn("rank", dense_rank() over(windowspec)).filter(col("rank")<=3)
    resultdf.show()


    print("spark sql")

    data.createOrReplaceTempView("datadf")
    spark.sql(
      """
         with cte as
        (
        select *,
        dense_rank() over(partition by Department, Year order by Salary desc) as drk
        from datad

        )
        select *
        from cte
        where drk <=3
        """).show()
  }
}

