import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.JdbcRDD
import scala.util.parsing.json.JSON
import net.liftweb.json._
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame

object ConnectDB {
	val CONNECTION_URL = "jdbc:postgresql://192.168.1.21/postgres?user=postgres&password=123456";

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
			implicit val formats = DefaultFormats
			val item = x.getString(6)
			val jValue = parse(item)
			val event = x.getString(1)
			val entityType = x.getString(2)

			val prop = jValue.extract[Properties]
			val jData = parse(prop.data)
			var data = jData.extract[Data]
			var productId = ""
			var algorithm = ""

			val timestamp = data.timestamp
			
			var listProduct = List[String]()
			if("REC" == event){
				try{
					val data3 = jData.extract[RecData]
					listProduct = data3.listProduct
					algorithm = data3.algorithm
				}catch{ 
					case e: Throwable => Nil
				}
			}else if ("BUY" == event){
				try{
					val data2 = jData.extract[BuyData]
					listProduct = data2.products.map(x => x.productID)
				}catch{
					case e: Throwable => println(e); Nil
				}
			}else if ("VIEW" == event){
				try{
					val data2 = jData.extract[OtherDataFromRec]
					val product = data2.product
					productId = product.productID
					algorithm = data2.algorithm
				}catch{
					case e: Throwable => println(e); Nil
				}
			}else{
				try{
					val data2 = jData.extract[OtherData]
					val product = data2.product
					productId = product.productID
				}catch{
					case e: Throwable => println(e); Nil
				}
				
			}
			lazy val entry = RawLogEntry(event,
				entityType,
				timestamp,
				productId,
				listProduct,
				algorithm)
			entry
	}
	def loadDB(sc : SparkContext, tsFromInSecond: Long, tsToInSecond: Long) = {
		val sqlContext = new SQLContext(sc)
		import sqlContext.implicits._
		// val url = "jdbc:postgresql://192.168.1.21/?user=postgres&password=123456"

		val people = sqlContext.read.format("jdbc").options(Map(
		 	"url" -> CONNECTION_URL,
		 	"dbtable" -> "pio_event_1",
		 	"driver" -> "org.postgresql.Driver")).load();
		val kz = people.filter(people.col("eventtime").cast("long").geq(tsFromInSecond))
						.filter(people.col("eventtime").cast("long").leq(tsToInSecond))
		val lst = kz.rdd
		val rest = lst.map(
			x =>(genKey(x), genValue(x))
		)
		rest.collect().toList
	}
	def saveDB(sc : SparkContext, r7: List[(String, Int, Int, Int, Int, Int, java.sql.Timestamp, java.sql.Timestamp)]) ={
		val sqlContext = new SQLContext(sc)
		import sqlContext.implicits._
		val distData = sc.parallelize(r7)
		val res = sqlContext.createDataFrame(distData)
    	val rows = res.collect()
		res.insertIntoJDBC(CONNECTION_URL, "metric_result", false);
	}
}