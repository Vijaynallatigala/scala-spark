import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object df {

  def main(args:Array[String]):Unit= {

    println("helloworld")

    val spark = SparkSession.builder()
                .appName("sparkprogram")
                .master("local[*]")
                .getOrCreate()

    //DDL

    //val ddlSchema="ID Int, Name String, Age Int, City String, Score Int"

    //    val pgschema = StructType(List(
    //
    //      StructField("ID", IntegerType),
    //      StructField("Name", StringType),
    //      StructField("Age", IntegerType),
    //      StructField("city", StringType),
    //      StructField("Score", IntegerType)
    //
    //    ))

    val df = spark.read
      .format("csv")
      //      .schema(pgschema)
      .option("header", "true")
      .load("file:///C://Users/User/Desktop/details.csv")
    df.show(false)
    df.printSchema()

    df.createOrReplaceTempView("vijay")

    spark.sql("SELECT * FROM vijay").show()
  }

}
