import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.JdbcRDD
import scala.util.parsing.json.JSON
import net.liftweb.json._
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object ConnectDB {
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
	def loadDB() = {
		val sc = new SparkContext("local", "Rec count", "/opt/spark", List("target/scala-2.10/simple-project_2.10-1.0.jar"))
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
		rest.collect().toList
	}
}