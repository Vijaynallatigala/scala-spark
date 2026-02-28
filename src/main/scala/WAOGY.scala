import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, current_date, datediff, initcap, lit, month, sum, to_date, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.immutable.Nil.groupBy

object WAOGY {

  def main(args: Array[String]): Unit = {

    print("helloworld")

    val spark = SparkSession.builder()
      .appName("sparkapplication")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    //    Create a DataFrame that lists employees with names and their work status. For each employee,
    //    determine if they are “Active” or “Inactive” based on the last check-in date. If the check-in date is
    //    within the last 7 days, mark them as "Active"; otherwise, mark them as "Inactive." Ensure the first
    //      letter of each name is capitalized.

    //    print("scala spark")
    //
    //    val employees = List(
    //      ("karthik", "2026-01-24"),
    //      ("neha", "2024-10-20"),
    //      ("priya", "2024-10-28"),
    //      ("mohan", "2024-11-02"),
    //      ("ajay", "2024-09-15"),
    //      ("vijay", "2024-10-30"),
    //      ("veer", "2024-10-25"),
    //      ("aatish", "2024-10-10"),
    //      ("animesh", "2024-10-15"),
    //      ("nishad", "2024-11-01"),
    //      ("varun", "2024-10-05"),
    //      ("aadil", "2024-09-30")
    //    ).toDF("name", "last_checkin")
    //
    //    employees.select (
    //      initcap(col("name")).as("name"),col("last_checkin"),when(
    //        datediff(current_date(),col("last_checkin"))<=7, lit("Active")).otherwise("Inactive").as("status")
    //
    //    ).show()
    //
    //    print("spark sql")
    //
    //
    //
    //
    //    employees.createOrReplaceTempView("employeesdf")
    //    spark.sql(
    //      """
    //        select  initcap(name) as name, to_date(last_checkin) as last_checkin,
    //        case
    //         when datediff(current_date(), last_checkin) <= 7 then "Active"
    //         else "InActive"
    //        end as status
    //        from employeesdf
    //
    //        """
    //    ).show()

    //    print("scala spark")
    //
    //    val sales = List(
    //      ("karthik", 60000),
    //      ("neha", 48000),
    //      ("priya", 30000),
    //      ("mohan", 24000),
    //      ("ajay", 52000),
    //      ("vijay", 45000),
    //      ("veer", 70000),
    //      ("aatish", 23000),
    //      ("animesh", 15000),
    //      ("nishad", 8000),
    //      ("varun", 29000),
    //      ("aadil", 32000)
    //    ).toDF("name", "total_sales")
    //
    //    val results = sales.select(
    //         initcap(col("name")) as ("name"),col("total_sales"),
    //         when(col("total_sales")> 50000, "excellent")
    //         .when(col("total_sales").between(25000,50000),"Good")
    //         .otherwise("Needs Improvement").as("performance_status"))
    //           .groupBy(("performance_status")).agg(sum(col(("total_sales")))).show()
    //
    //
    //    print("spark sql")
    //
    //    sales.createOrReplaceTempView("salesdf")
    //    spark.sql(
    //      """
    //         with cte as
    //        (
    //        select initcap(name) as name, total_sales,
    //        case
    //        when total_sales > 50000 then 'excellent'
    //        when total_sales between 25000 and 50000 then 'Good'
    //        else 'needs improvement'
    //        end as performance_status
    //        from salesdf
    //        )
    //        select performance_status,sum(total_sales) as total_sales
    //        from cte
    //        group by performance_status
    //        """
    //    ).show()


    //    print("scala spark")
    //
    //    val workload = List(
    //      ("karthik", "ProjectA", 120),
    //      ("karthik", "ProjectB", 100),
    //      ("neha", "ProjectC", 80),
    //      ("neha", "ProjectD", 30),
    //      ("priya", "ProjectE", 110),
    //      ("mohan", "ProjectF", 40),
    //      ("ajay", "ProjectG", 70),
    //      ("vijay", "ProjectH", 150),
    //      ("veer", "ProjectI", 190),
    //      ("aatish", "ProjectJ", 60),
    //      ("animesh", "ProjectK", 95),
    //      ("nishad", "ProjectL", 210),
    //      ("varun", "ProjectM", 50),
    //      ("aadil", "ProjectN", 90)
    //    ).toDF("name", "project", "hours")
    //
    //    val resultdf = workload.select(initcap(col("name")).as ("name"),
    //        col("project"),
    //        col("hours"),
    //        when(col("hours")>200,"overloaded")
    //        .when(col("hours").between(100,200),"balanced")
    //        .otherwise("underutilized")
    //          .as("category"))
    //      .groupBy("category").agg(count("*").as("employee_count"))
    //      .show()
    //
    //    print("spark sql")
    //
    //    workload.createOrReplaceTempView("workloaddf")
    //    spark.sql(
    //      """
    //         with cts as
    //         (
    //         select initcap(name) as name, project, hours,
    //         case
    //         when hours > 200 then 'overloaded'
    //         when hours between 100 and 200 then 'balanced'
    //         else 'underutilized'
    //         end as category
    //        from workloaddf
    //        )
    //        select category,count(*) as employee_count
    //        from cts
    //        group by category
    //        order by employee_count desc
    //        """).show()

    //    print("scala spark")
    //
    //    val employees = List(
    //      ("karthik", 62),
    //      ("neha", 50),
    //      ("priya", 30),
    //      ("mohan", 65),
    //      ("ajay", 40),
    //      ("vijay", 47),
    //      ("veer", 55),
    //      ("aatish", 30),
    //      ("animesh", 75),
    //      ("nishad", 60)
    //    ).toDF("name", "hours_worked")
    //
    //    val empdf = employees.select(initcap(col("name")).as("name"),col("hours_worked")
    //                    ,when(col("hours_worked")>60, "Excessive_Overtime")
    //                    .when(col("hours_worked").between(45,60),"standard_overtime").otherwise("No_Overtime").as("over_time_status"))
    //                    .groupBy("over_time_status").agg(count("*")).as("employee_count")
    //    empdf.show()
    //
    //    print("spark sql")
    //
    //    employees.createOrReplaceTempView("employeedf")
    //    spark.sql(
    //      """
    //        with cte as
    //        (
    //        select initcap(name) as name, hours_worked,
    //        case
    //        when hours_worked > 60 then "Excessive_Overtime"
    //        when hours_worked between 45 and 60 then "standard_overtime"
    //        else "No_Overtime"
    //        end as Over_time_status
    //        from employeedf
    //        )
    //        select over_time_status, count(*) as employee_count
    //        from cte
    //        group by over_time_status
    //        order by employee_count desc
    //        """).show()

    //print("scala spark")
    //
    //    val customers = List(
    //      ("karthik", 22),
    //      ("neha", 28),
    //      ("priya", 40),
    //      ("mohan", 55),
    //      ("ajay", 32),
    //      ("vijay", 18),
    //      ("veer", 47),
    //      ("aatish", 38),
    //      ("animesh", 60),
    //      ("nishad", 25)
    //    ).toDF("name", "age")
    //
    //    val custdf = customers.select(initcap(col("name")).as("name"),col("age")
    //                          ,when(col("age")<25,"Youth")
    //                          .when(col("age").between(25,45),"Adult")
    //                          .otherwise("senior").as("category"))
    //      .groupBy("category").agg(count("*").as("total_customers"))
    //    custdf.show()
    //
    //    print("spark sql")
    //    customers.createOrReplaceTempView("customerdf")
    //
    //    spark.sql(
    //      """
    //       with cte as (
    //        select initcap(name) as name, age,
    //        case
    //        when age < 25 then "Youth"
    //        when age between 25 and 45 then "Adult"
    //        else "Senior"
    //        end as Category
    //        from customerdf
    //        )
    //        select Category, count(*) as total_customers
    //        from cte
    //        group by Category
    //        order by total_customers desc
    //
    //        """).show()
    //

    print("scala spark")

    val customers = List(
      ("karthik", 22),
      ("neha", 28),
      ("priya", 40),
      ("mohan", 55),
      ("ajay", 32),
      ("vijay", 18),
      ("veer", 47),
      ("aatish", 38),
      ("animesh", 60),
      ("nishad", 25)
    ).toDF("name", "age")


    def customersAgeGroupSummary(customers: DataFrame): DataFrame = {

      customers.select(initcap(col("name")).as("name"), col("age"),
        when(col("age") < 25, "youth").when(col("age").between(25, 45), "adult")
          .otherwise("senior").as("category")
      ).groupBy("category").agg(count("*").as("total_customers"))

    }

    val resultdf = customersAgeGroupSummary(customers)
    resultdf.show()

    print("spark sql")
    customers.createOrReplaceTempView("custdf")
    spark.sql(
      """
         with cte as (

      select initcap(name) as name, age,
      case
      when age < 25 then "young"
      when age between 25 and 45 then "adult"
      else "senior"
      end as category
      from custdf
      )
      select category,count(*) as total_customers
      from cte
      group by category
      order by total_customers desc


       """).show()




  }
}






