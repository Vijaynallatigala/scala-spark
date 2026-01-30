
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, month, to_date, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object df2 {

  def main(args: Array[String]): Unit = {

    print("helloworld")

    val spark = SparkSession.builder()
      .appName("sparkapplication")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    //
    //    val df = spark.read
    //      .format("csv")
    //      //      .schema(pgschema)
    //      .option("header", "true")
    //      .load("file:///C://Users/User/Desktop/details.csv")
    //    df.show(false)
    //    df.printSchema()

    // when and otherwise problems
    // print("scala_spark")
    //
    //   val employees = List (
    //     (1, "AJAY", 28),
    //     (2, "VIJAY", 17),
    //     (3, "MANOJ", 22)
    //   ).toDF("id", "name", "age")
    //
    //    employees.show()
    //
    //    //scala spark
    //    val result_df = employees.withColumn("is_adult",when(col("age")>=18,true).otherwise(false))
    //
    //    result_df.show()
    //
    //    //sql spark
    //
    //    print("spark_sql")
    //
    //    employees.createOrReplaceTempView("employeedf")
    //
    //    spark.sql(
    //      """
    //        Select
    //        id,
    //        name,
    //        case
    //            when age>=18 then true
    //            else false
    //            end as is_adult
    //            from employeedf
    //        """
    //    ).show()

    // 2)Categorizing Values: Data: A DataFrame grades with columns student_id, score.

    //    Question: How would you add a new column grade with values "Pass" if score is greater than or
    //    equal to 50, and "Fail" otherwise?

    //    print("spark scala")
    //
    //    val grades = List(
    //      (1, 85),
    //      (2, 42),
    //      (3, 73)
    //    ).toDF("student_id", "score")
    //
    //    val result_df = grades.withColumn("status", when(col("score")>=50,"pass").otherwise("false")).show()
    //
    //
    //    print("sql spark")
    //
    //    grades.createOrReplaceTempView("gradesdf")
    //    spark.sql(
    //      """
    //        select student_id,score,
    //
    //        case
    //           when score>=50 then "pass"
    //           else "fail"
    //           end as status
    //        from gradesdf
    //        """
    //    ).show()


    //3)Multiple Conditions: Data: A DataFrame transactions with columns transaction_id, amount.

    //Question: How would you add a new column category with values "High" if amount is greater than
    //1000, "Medium" if amount is between 500 and 1000, and "Low" otherwise?

    //    print("spark-scala")
    //
    //    val transactions = List(
    //      (1, 1000),
    //      (2, 200),
    //      (3, 5000)
    //    ).toDF("transaction_id", "amount")
    //
    //    transactions.show()
    //
    //val resultdf = transactions.withColumn("status", when(col("amount")>=1000,"High").when(col("amount").between (1000,500),"medium").otherwise("low")).show()
    //
    //
    //    print("spark-sql")
    //
    //    transactions.createOrReplaceTempView("transactionsdf")
    //    spark.sql(
    //      """
    //      select transaction_id,
    //      amount,
    //      case
    //      when amount >= 1000 then "high"
    //      when amount between 1000 and 500 then "medium"
    //      else "low"
    //      end as status
    //      from transactionsdf
    //      """
    //    ).show()


//    print("spark - scala")
//
//    val products = List(
//      (1, 30.5),
//      (2, 150.75),
//      (3, 75.25)
//    ).toDF("product_id", "price")
//    products.show()
//
//  print("spark -scala")
//    val resultdf = products.withColumn("status" ,when(col("price")< 50, "cheap").when(col("price").between(50, 100), "moderate").otherwise("expensive")).show()
//
//    print("spark -sql")
//    products.createOrReplaceTempView("productsdf")
//
//    spark.sql(
//      """
//        select product_id, price,
//        case
//        when price<50 then "cheap"
//        when price between 50 and 100 then "moderate"
//        else "expensive"
//        end as status
//        from productsdf
//        """
//    ).show()

// print("spark scala")
//    val events = List(
//      (1, "2024-07-27"),
//      (2, "2024-12-25"),
//      (3, "2025-01-01")
//
//    ).toDF("event_id", "date")
//
//    events.show()


//    val resultdf = events.withColumn("is_holiday", when(col("date")==="2024-12-25" || col("date")=== "2025-01-01","true").otherwise("false"))
//    resultdf.show()
//
//    print("spark -- sql")
//
//    resultdf.createOrReplaceTempView("resultdff")
//    spark.sql(
//      """
//        select event_id, date,
//        case
//        when date='2024-12-25'
//         or date== '2025-01-01'
//          then true
//        else false
//        end as is_holiday
//        from resultdff
//        """).show()

    //  1. A dataframe inventory with columns item_id, quantity.

    // 1. How would you add a new column stock_level with values "low" if quantity is less than 10, "medium"
    // if quantity is between 10 and 20, and "high" otherwise?

//    print("scala spark")
//
//    val inventory = List (
//      (1,5),
//      (2,15),
//      (3,25)
//    ).toDF("item_id", "quantity")
//    inventory.show()
//
//    val resultdf = inventory.withColumn("stock_level", when(col("quantity")<10,"low").when(col("quantity").between (10, 20), "medium").otherwise("high"))
//    resultdf.show()
//
//    print("sql_spark")
//
//    resultdf.createOrReplaceTempView("resultdf1")
//
//    spark.sql(
//      """
//        select item_id, quantity,
//        case
//        when quantity < 10 then 'low'
//        when quantity between 10 and 20 then 'medium'
//        else 'high'
//        end as stock_level
//        from resultdf1
//        """
//    ).show()

//print("spark scala")
//
//    val customers = List(
//      (1, "john@gmail.com"),
//      (2, "jane@yahoo.com"),
//      (3, "doe@hotmail.com")
//    ).toDF("customer_id", "email")
//
//    customers.show()
//
//    val result_df = customers.withColumn("email_provider",
//                             when(col("email").contains ("gmail"), "gmail")
//                               .when(col("email").contains ("yahoo"),"yahoo")
//                               .otherwise("other"))
//
//    result_df.show()
//
//    print("spark sql")
//
//    customers.createOrReplaceTempView("customersdf")
//
//    spark.sql(
//      """
//        select "customer_id", "email",
//        case
//        when email like '%gmail%' then 'gmail'
//        when email like '%yahoo%' then 'yahoo'
//        else 'other'
//        end as email_provider
//        from customersdf
//        """
//    ).show()


//    A dataframe orders with columns order_id, order_date

    //    how would you add a new column season with values "summer" if order_date is in june, july, or august, "winter" if in december, january, or february, and "other" otherwise?


//    print("spark scala")
//
//    val orders = List(
//      (1, "2024-07-01"),
//      (2, "2024-12-01"),
//      (3, "2024-05-01")
//    ).toDF("order_id",("order_date"))
//
//    //orders.show()
//
//    val orderdf = orders.withColumn("season",when(month(to_date(col("order_date"))).isin(6,7,8),"summer")
//                                            .when(month(to_date(col("order_date"))).isin(12,1,2),"winter")
//                                             .otherwise("other"))
//      orderdf.show()
//
//
//
//    print("spark - sql")
//
//    orders.createOrReplaceTempView("ordersdf")
//
//    spark.sql(
//      """
//         select order_id, order_date,
//         case
//         when month(to_date(order_date)) IN (6,7,8) then 'summer'
//         when month(to_date(order_date)) IN (12,1,2) then 'winter'
//         else 'other'
//         end as season
//         from ordersdf
//
//        """
//    ).show()




    //Multiple Nested Conditions: Data: A DataFrame sales with columns sale_id, amount.

//    Question: How would you add a new column discount with values 0 if amount is less than 200, 10 if
//    amount is between 200 and 1000, and 20 if amount is greater than 1000?

//    print("scala spark")
//
//    val sales = List(
//      (1, 100),
//      (2, 1500),
//      (3, 300)
//    ).toDF("sale_id", "amount")
//
//    val salesdf = sales.withColumn("discount", when(col("amount") < 200, "0").when(col("amount").between (200,1000),"10").otherwise("20"))
//    salesdf.show()
//
//    print("spark sql")
//
//    sales.createOrReplaceTempView("salesdff")
//    spark.sql(
//      """
//         select sale_id, amount,
//         case
//         when amount < 200 then "0"
//         when amount between 200 and 1000 then "10"
//         else "20"
//         end as discount
//         from salesdff
//
//        """
//    ).show()


//    5)Conditional Column with Boolean Values: Data: A DataFrame logins with columns login_id,
//    login_time.

//    Question: How would you add a new column is_morning which is true if login_time is before 12:00,
//    and false otherwise?

//    print("scala spark")
//
//    val logins = List(
//      (1, "09:00"),
//      (2, "18:30"),
//      (3, "14:00")
//    ).toDF("login_id", "login_time")
//
//    val logindf = logins.withColumn("is_morning",when(col("login_time") < "12:00", true).otherwise(false))
//    logindf.show()
//
//    print("sql spark")
//
//    logins.createOrReplaceTempView("loginsdf")
//
//    spark.sql(
//      """
//        select login_id, login_time,
//        case
//        when "login_time < "12:00" then "true"
//        else "false"
//        end as is_morning
//      from
//        """
//    )

   // print("scala spark")

    // A DataFrame employees with columns employee_id, age,
    //salary

    // How would you add a new column category with values "Young & Low Salary" if age is less
    //than 30 and salary is less than 35000, "Middle Aged & Medium Salary" if age is between 30 and 40
    //and salary is between 35000 and 45000, and "Old & High Salary" otherwise?


//    val employees = List(
//      (1, 25, 30000),
//      (2, 45, 50000),
//      (3, 35, 40000)
//    ).toDF("employee_id", "age", "salary")
//
//
//    val resultdf = employees.withColumn("category", when(col("age") < 30 && col("salary") < 35000, "young & low_salary")
//                                                 .when(col("age").between(30, 40) && col("salary").between(35000, 45000), "middle Aged & medium salary")
//                                                 .otherwise("Old & High Salary"))
//
//      resultdf.show(false)
//
//
//    print("sql spark")
//
//    employees.createOrReplaceTempView("employeesdf")
//
//    spark.sql(
//      """
//        select employee_id, age, salary,
//        case
//          when age < 30 AND salary < 35000 then "young & low_salary"
//          when age between 30 AND 40 AND salary between 35000 AND 45000 then "middle Aged & medium salary"
//          else "Old & High Salary"
//        end as category
//
//        from employeesdf
//
//        """).show(false)


//    print("scala spark")
//
////    A DataFrame reviews with columns review_id, rating.
////
////    Question: How would you add two new columns, feedback with values "Bad" if rating is less than 3,
////    "Good" if rating is 3 or 4, and "Excellent" if rating is 5, and is_positive with values true if rating is
////      greater than or equal to 3, and false otherwise?
//
//    val reviews = List(
//          (1, 1),
//          (2, 4),
//         (3, 5)
//       ).toDF("review_id", "rating")
//
//    val resultdf = reviews.withColumn("feedback", when(col("rating")< 3,"bad")
//                                             .when(col("rating").between(3, 4),"good")
//                                             .otherwise("excellent"))
//      .withColumn("is_positive",when(col("rating")>=3,"true").otherwise("false"))
//
//    resultdf.show()





  }

}