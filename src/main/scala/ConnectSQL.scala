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
case class Data(timestamp: Long, sessionId: String,action: String)
case class OtherData(timestamp: Long, sessionId: String,action: String, product:Product)
case class RecData(timestamp: Long, sessionId: String,action: String, listProduct:List[String])

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
	def genValue(x: Row) ={
		//can tinh:
		//so lan moi san pham duoc recommended
		//so lan moi san pham duoc VIEW sau lan recommend dau tien
		//so lan moi san pham duoc BUY sau lan recommend dau tien
			implicit val formats = DefaultFormats
			val item = x.getString(6)
			val jValue = parse(item)
			val event = x.getString(1)
			val entityType = x.getString(2)

			val prop = jValue.extract[Properties]
			val jData = parse(prop.data)
			var data = jData.extract[Data]
			var productId = ""
			try{
				val data2 = jData.extract[OtherData]
				val product = data2.product
				productId = product.productID
			}catch{
				case e => Nil
			}
			val timestamp = data.timestamp
			
			var listProduct = List("")
			if(event == "REC"){
				try{
					val data3 = jData.extract[RecData]
					listProduct = data3.listProduct
				}catch{
					case e => Nil
				}
			}else{
				
			}
			lazy val entry = RawLogEntry(event,
				entityType,
				timestamp,
				productId,
				listProduct)
			entry
	}
	def mergeMap[A, B](ms: List[Map[A, B]])(f: (B, B) => B): Map[A, B] =
	  (Map[A, B]() /: (for (m <- ms; kv <- m) yield kv)) { (a, kv) =>
	    a + (if (a.contains(kv._1)) kv._1 -> f(a(kv._1), kv._2) else kv)
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
		val rest = lst.map(
			x =>(genKey(x), genValue(x))
		)
		//rest.foreach(p => println(">>> key=" + p._1 + ", value=" + p._2))
		val s = for(p <- rest) yield Map(p._1 -> p._2)
		val sm =rest.collect().toList
		val s2 = sm.map(x=> (x._1, List(x._2)))
		// println(s2)
		val s3 = sc.parallelize(s2).reduceByKey((x, y) => x ::: y)
//		s3.foreach(println)
		//val s4 = for(p <- s3) yield Map(p._1 -> p._2.timestamp)
		// val s4 = for(p <- s3) yield (for(q <- p._2) yield Map(q.timestamp -> q))
		val s5 = for(p <- s3) yield (for{
			q <- p._2
			val x = (q.productID -> q)
			} yield x)
		s5.foreach(println)
		 /*var ws = for{
		 	p <- s5
			 val w = (p.reduce((x,y) => {
			 		(x._1, RawLogEntry(x._2.event,
				x._2.entityType,
				x._2.timestamp,
				x._2.productID,
				x._2.listProduct))
			 		}))
		 } yield w
		 println(ws)*/
		// val mm=mergeMap(s.collect().toList)((v1, v2) => {
		// 	RawLogEntry(v1.event,
		// 		v1.entityType,
		// 		v1.timestamp - v2.timestamp,
		// 		"v1.productId",
		// 		List("pr","p2"))
		// 	}
		// )
		// mm.foreach(println)
	}
}
