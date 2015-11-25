import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.JdbcRDD
import scala.util.parsing.json.JSON
import net.liftweb.json._
import scala.collection.mutable.ListBuffer

case class Properties(data: String)
case class Product(productID: String)
case class OtherData(timestamp: Long, sessionId: String,action: String, listProduct: String*)
case class RecData(timestamp: Long, sessionId: String,action: String, listProduct: String*)

object WordCount {

	def main(args: Array[String]){
		val sc = new SparkContext("local", "Word Count", "/opt/spark", List("target/scala-2.10/simple-project_2.10-1.0.jar"))
		val sqlContext = new SQLContext(sc)
		import sqlContext.implicits._
		val url = "jdbc:postgresql://192.168.1.21/?user=postgres&password=123456"
		val commits = sqlContext.load("jdbc", Map(
		 	"url" -> url,
		 	"dbtable" -> "pio_event_1",
		 	"driver" -> "org.postgresql.Driver"))
		println("......finished....1...")

		// val a = commits.filter("event = \"REC\"").select("properties")
		val a = commits.select("properties")

		var lb = new ListBuffer[Data]()
		implicit val formats = DefaultFormats
		a.collect.foreach(i => {
			val item = i.getString(0)
			val jValue = parse(item)
			val prop = jValue.extract[Properties]
			val jData = parse(prop.data)
			val data = jData.extract[RecData]
			lb += data
		})
		val listRecord = lb.toList
		lb.foreach(println)
		// for( item <- a){
		// 	val listProduct = JSON.parseFull(item.getString(0)) match {
		// 		case Some(x) => {
		// 			val m = x.asInstanceOf[Map[String, String]]
		// 			println(m)
		// 		}
		// 	}
		// }
		println("......finished....2...")

		// val myRDD = new JdbcRDD( sc, () => 
  //                              DriverManager.getConnection(url,"postgres","123456"),
  //                       "select event,entitytype from pio_event_1 limit ?, ?",
  //                       1,//lower bound
  //                       5,//upper bound
  //                       2,//number of partitions
  //                       r =>
  //                         r.getString("event") + ", " + r.getString("entitytype"))
	}
}
