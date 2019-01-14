import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {
    val inputFile =  "hdfs://10.1.100.102:9000/user/alphasta/test/words.txt"
    val conf = new SparkConf().setAppName("WordCount").setMaster("spark://10.1.100.102:7077")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
    println("=====================11111111========================")
    val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    println("= "+wordCount.count())
    wordCount.foreach(println)
    println("=====================22222222========================")
  }
}