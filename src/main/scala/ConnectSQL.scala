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

case class Result(productID: String, totalView: Int,totalRec: Int)


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
			
			var listProduct = List[String]()
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
   /* def fnSsn(lstRawLog: List[(String, RawLogEntry)]): List[Result] = {
    	val s1 = lstRawLog.groupBy(x => x._2)
    	println(s1)
		val x = List(Result("10001",3,10), Result("10005",1,2))
		x
    }*/
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
		val s = for(p <- rest) yield Map(p._1 -> p._2)
		val sm =rest.collect().toList
		val s2 = sm.map(x=> {
			if(x._2 != Nil){
				(x._1, List(x._2))
			}
			else (x._1, List())
		})
		// val s3 = sc.parallelize(s2).reduceByKey((x, y) => x ::: y)

		// val s5 = for(p <- s3) yield (for{
		// 	q <- p._2
		// 	val x = (q.productID -> q)
		// 	} yield x)
// 
		// sm.foreach(println)

         val rst = sm.groupBy( _._1 ).map( kv => (kv._1, kv._2.map( x=> {
            if(x._2.listProduct.isEmpty) List((x._2.productID.toString,x._2.event, x._2.timestamp))
            else{
	            for{
	                item <- x._2.listProduct
	                val k = (item, x._2.event, x._2.timestamp)
	              } yield k
			}
          }).flatten.groupBy(_._1).map(k =>(k._1, k._2.map(k=> k._2)))))

        rst.foreach(println)

		// println(fnSsn(sm))
	}
}
