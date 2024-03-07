import org.apache.spark.{SparkConf, SparkContext}
object SparkExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkExample").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(1 to 10)
    println(rdd.collect().mkString(", "))

    sc.stop()
  }
}