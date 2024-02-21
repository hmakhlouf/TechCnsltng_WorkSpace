object Main {
  def main(args: Array[String]): Unit = {
    println("Hello world!")
    val rdd = sc.parallelize (1 to 10)
  }
}