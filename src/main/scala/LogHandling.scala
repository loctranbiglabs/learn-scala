/*import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object WordCount {
	def main(args: Array[String]){
		val inputFile = "src/data/LogAddToWishlist12.txt"
		val outputDir = "src/data/wc-output"

		val sc = new SparkContext("local", "Word Count", "/opt/spark", List("target/scala-2.10/simple-project_2.10-1.0.jar"))
		val input = sc.textFile(inputFile)
		println("input %s ".format(input))

		val words = input.flatMap(line => line.split(" "))
		println("words %s ".format(words))
		val counts = words.map(word => (word, 1)).reduceByKey{case (x,y) => x+y}

		counts.saveAsTextFile(outputDir)
		println("counts %s ".format(counts))
	}
}
*/