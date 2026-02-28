import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, desc, rank, sum}


object windowagg3 {

  def main(args:Array[String]):Unit={

    val spark = SparkSession.builder().appName("windowagg3").master("local[*]").getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    // Finding the maximum revenue for each product category and the corresponding product.

    print("scala spark")

//    val salesData = Seq(
//      ("Product1", "Category1", 100),
//      ("Product2", "Category2", 200),
//      ("Product3", "Category1", 150),
//      ("Product4", "Category3", 300),
//      ("Product5", "Category2", 250),
//      ("Product6", "Category3", 180)
//    ).toDF("Product", "Category", "Revenue")
//
//    val windowspec = Window.partitionBy("Category").orderBy(col("Revenue").desc)
//
//    val result = salesData.withColumn("rank", rank().over(windowspec)).filter(col("rank")=== 1).drop("rank")
//    result.show()
//
//    print("spark sql")
//
//    salesData.createOrReplaceTempView("salesdf")
//    spark.sql(
//      """
//        select Product, Category, Revenue
//        from
//        (select *,
//        rank() over(partition by Category order by Revenue desc) as rnk
//        from salesdf) as t
//        where rnk = 1
//        """).show()

    // Calculating the percentage contribution of each product's revenue to the total revenue in its
    //category
//
//    val salesData = Seq(
//      ("Product1", "Category1", 100),
//      ("Product2", "Category1", 200),
//      ("Product3", "Category1", 150),
//      ("Product4", "Category2", 300),
//      ("Product5", "Category2", 250),
//      ("Product6", "Category2", 180)
//    ).toDF("Product", "Category", "Revenue")
//
//
//    val windowSpec = Window.partitionBy("Category")
//
//    val categoryTotalRevenue = sum("Revenue").over(windowSpec)
//    val revenueContribution = salesData.withColumn("TotalRevenue", categoryTotalRevenue)
//      .withColumn("Contribution", col("Revenue") / col("TotalRevenue") * 100)
//    revenueContribution.show()
//
//    print("spark sql")
//
//    salesData.createOrReplaceTempView("salesdf")
//    spark.sql(
//      """
//        select Product, Category, Revenue,
//        SUM(Revenue) OVER (PARTITION BY Category) AS TotalRevenue,
//           (Revenue / SUM(Revenue) OVER (PARTITION BY Category)) * 100 AS Contribution
//    FROM salesdf
//
//        """).show()

    //Calculating the moving average of revenue for each product over a rolling window of size 2.


    val salesData = Seq(
      ("Product1", 100),
      ("Product1", 150),
      ("Product1", 200),
      ("Product1", 180),
      ("Product1", 250),
      ("Product1", 300)
    ).toDF("Product", "Revenue")

    val windowSpec = Window.partitionBy("Product").orderBy("Revenue").rowsBetween(-1, 0)
    val movingAverage = salesData.withColumn("MovingAverage", avg("Revenue").over(windowSpec))
    movingAverage.show()


    salesData.createOrReplaceTempView("salesdf")

    spark.sql(
      """
    SELECT Product,
           Revenue,
           AVG(Revenue) OVER (
               PARTITION BY Product
               ORDER BY Revenue
               ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
           ) AS MovingAverage
    FROM salesdf
  """
    ).show()

  }

}
