import org.apache.spark.sql.SparkSession

object sparkApp2 {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("My Spark Application")
      .config("spark.master", "local") // Use local in a development environment
      .getOrCreate()

    // Example Spark transformation: creating a DataFrame and showing its content
    val dataSeq = Seq(("Alice", 1), ("Bob", 2), ("Charlie", 3))
    val df = spark.createDataFrame(dataSeq).toDF("name", "id")

    df.show()

    // Always stop SparkSession at the end
    spark.stop()
  }

}