import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object datacleaning {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DateDiffExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    print("scala spark")



    val schema = StructType(Array(
      StructField("emp_id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("department", StringType, true),
      StructField("salary", StringType, true),
      StructField("joining_date", StringType, true)
    ))

    val df = spark.read
      .format("csv")
      .option("header","true")
      .option("inferSchema","false")
      .schema(schema)
      .load("C:/Users/User/Downloads/emp_data.csv")

    df.show(false)

  print("drop duplicatees")

    val df1 = df.dropDuplicates("name")
    df1.show()


    print("distinct")
    val df2 = df.select("department").distinct()
    df2.show()


    print("renaming columns")

    val df3 = df.withColumnRenamed("name", "full_name")
    df3.show()


    print("handling dates")

    val df4 = df.withColumn(
      "joining_date",
      coalesce(
        to_date(col("joining_date"), "yyyy-MM-dd"),
        to_date(col("joining_date"), "dd-MM-yyyy"),
        to_date(col("joining_date"), "yyyy/MM/dd")
      )
    )
    df4.show(false)
    df4.printSchema()

    print("type casting")

    val df5 = df.withColumn("salary", col("salary").cast("integer"))
    df5.show()
    df5.printSchema()

    print("trimming strings")

    val df6 = df.withColumn("name", ltrim(col("name")))
    df6.show()
    val df7 = df.withColumn("name", rtrim(col("name")))
    df7.show()
    val df8 = df.withColumn("name", ltrim(col("name")))
        df8.show()

    print("filtering data")

    val df9 = df.select("name", "department").filter(col("salary")> 65000)
    df9.show()

  print("null")
    val nulldf = df.select("name", "department").filter(col("name") === "NULL")
    nulldf.show()





  }

}




