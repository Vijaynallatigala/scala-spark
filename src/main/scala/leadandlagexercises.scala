import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}

object leadandlagexercises {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DateDiffExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    print("scala spark")

    val schema = StructType(Seq(
      StructField("EmployeeName", StringType, true),
      StructField("Month", StringType, true),
      StructField("Year", IntegerType, true),
      StructField("Sales", FloatType, true),
      StructField("City", StringType, true),
      StructField("Department", StringType, true)
    ))

    val data =Seq(
    ("Ravi Verma", "Jan", 2023, 12000.0, "Delhi", "Electronics"),
    ("Ravi Verma", "Feb", 2023, 15000.0, "Delhi", "Electronics"),
    ("Ravi Verma", "Mar", 2023, 13000.0, "Delhi", "Electronics"),
    ("Pooja Mehta", "Jan", 2023, 10000.0, "Mumbai", "Clothing"),
    ("Pooja Mehta", "Feb", 2023, 12500.0, "Mumbai", "Clothing"),
    ("Pooja Mehta", "Mar", 2023, 11000.0, "Mumbai", "Clothing"),
    ("Amit Rathi", "Jan", 2023, 8000.0, "Delhi", "Grocery"),
    ("Amit Rathi", "Feb", 2023, 8500.0, "Delhi", "Grocery"),
    ("Amit Rathi", "Mar", 2023, 9000.0, "Delhi", "Grocery"),
    ("Neha Sinha", "Jan", 2023, 14000.0, "Chennai", "Electronics"),
    ("Neha Sinha", "Feb", 2023, 16000.0, "Chennai", "Electronics"),
    ("Neha Sinha", "Mar", 2023, 17000.0, "Chennai", "Electronics"),
    ("Suresh Yadav", "Jan", 2023, 9500.0, "Chennai", "Grocery"),
    ("Suresh Yadav", "Feb", 2023, 9800.0, "Chennai", "Grocery"),
    ("Suresh Yadav", "Mar", 2023, 10200.0, "Chennai", "Grocery"),
    ("Tanya Iyer", "Jan", 2023, 11000.0, "Mumbai", "Clothing"),
    ("Tanya Iyer", "Feb", 2023, 13000.0, "Mumbai", "Clothing"),
    ("Tanya Iyer", "Mar", 2023, 13500.0, "Mumbai", "Clothing")
    )

    val df = data.toDF(
      "EmployeeName",
      "Month",
      "Year",
      "Sales",
      "City",
      "Department"
    )
   // df.show(false)


    print("scala spark")

      //Get the previous month's sales for each employee using lag.

    val windowspec = Window.partitionBy("EmployeeName").orderBy(col("sales").desc())

    val lagdf = df.withColumn("prev_month_sales", lag("sales", 1).over(windowspec))
    lagdf.show()


  }

  }
