import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.JdbcRDD
import scala.util.parsing.json.JSON
import net.liftweb.json._
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

case class Properties(data: String)
case class Product(productID: String)
case class Data(timestamp: Long, sessionId: String,action: String, listProduct: String*)

object WordCount {
	def genKey(x: Row) ={
			implicit val formats = DefaultFormats
			val item = x.getString(6)
			val jValue = parse(item)
			val prop = jValue.extract[Properties]
			val jData = parse(prop.data)
			val data = jData.extract[Data]
			data.sessionId
	}
	def main(args: Array[String]){
		val sc = new SparkContext("local", "Word Count", "/opt/spark", List("target/scala-2.10/simple-project_2.10-1.0.jar"))
		val sqlContext = new SQLContext(sc)
		import sqlContext.implicits._
		val url = "jdbc:postgresql://192.168.1.21/?user=postgres&password=123456"
		val people = sqlContext.read.format("jdbc").options(Map(
		 	"url" -> url,
		 	"dbtable" -> "pio_event_1",
		 	"driver" -> "org.postgresql.Driver")).load()
		val lst = people.rdd
		val rest = lst.map{
			x => genKey(x) -> x
		}
		rest.foreach(p => println(">>> key=" + p._1 + ", value=" + p._2))
		// val lst = people.rdd match {
		// 	 case org.apache.spark.sql.Row(
		// 		id: String,
		// 		event: String,
		// 		entityType: String,
		// 		entityId: String,
		// 		targetEntityType: String,
		// 		targetEntityId: String,
		// 		properties: String,
		// 		eventTime: String,
		// 		eventTimezone: String,
		// 		tags: String,
		// 		prid: String,
		// 		createdTime: String,
		// 		createdTimezone: String
		// 		) => LogEntry(id,
		// 		event,
		// 		entityType,
		// 		entityId,
		// 		targetEntityType,
		// 		targetEntityId,
		// 		properties,
		// 		eventTime,
		// 		eventTimezone,
		// 		tags,
		// 		prid,
		// 		createdTime,
		// 		createdTimezone)
		// 	}
		// println(lst)
/*		val commits = sqlContext.load("jdbc", Map(
		 	"url" -> url,
		 	"dbtable" -> "pio_event_1",
		 	"driver" -> "org.postgresql.Driver"))
		println("......finished....1...")
		println()

		// val a = commits.filter("event = \"REC\"").select("properties")
		val a = commits.select("properties")

		var lb = new ListBuffer[Data]()
		implicit val formats = DefaultFormats
		a.collect.foreach(i => {
			val item = i.getString(0)
			val jValue = parse(item)
			val prop = jValue.extract[Properties]
			val jData = parse(prop.data)
			val data = jData.extract[Data]
			lb += data
		})
		val listRecord = lb.toList
	//	lb.foreach(println)
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
*/
	}
}
