 import org.apache.spark.SparkContext

  object rdd {
    def main( args:Array[String]):Unit={
      val sc = new SparkContext("local[*]", "sparkExample")
      val rdd1 = sc.textFile("C:\\Users\\User\\Desktop\\demo\\info.txt")
      val rdd2 = rdd1.flatMap(x=>x.split(" ")) //lambda
      val rdd3 = rdd2. map(x=>(x,1))
      val rdd4 = rdd3.reduceByKey((x,y)=>x+y)
      val rdd5=rdd4.sortBy(x=>x._2, false)
      rdd5.collect().foreach(println)

    }

}
