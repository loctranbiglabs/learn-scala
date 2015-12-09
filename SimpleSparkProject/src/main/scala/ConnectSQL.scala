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
case class OtherDataFromRec(timestamp: Long, sessionId: String,action: String, product:Product, algorithm: String)
case class RecData(timestamp: Long, sessionId: String,action: String, listProduct:List[String], algorithm: String)

case class Result(productID: String, totalView: Int,totalRec: Int)


object RecCount {
     def groupBySession(lst: List[(String, RawLogEntry)]) : List[(String, List[RawLogEntry])] = {
        lst.groupBy(_._1).toList.map(x => 
          (x._1, x._2.map(y => y._2))
        )
     }
     def findRelativeAlg(product: String, timestamp: Long, lst: List[RawLogEntry]): String = {
        val tmp = lst.filter(x => ("VIEW".equals(x.event) 
                                && product.equals(x.productID.toString) 
                                && x.timestamp < timestamp
                                && !x.algorithm.equals("")))
        if(tmp.size > 0) 
            tmp(0).algorithm
        else ""
     }
     def assignRelativeAction(lst: List[RawLogEntry]) : List[RawLogEntry] = {
        lst.map(x => {
          if("REC" != x.event && "VIEW" != x.event)
              RawLogEntry(x.event, x.entityType, x.timestamp, x.productID, x.listProduct, 
                findRelativeAlg(x.productID.toString, x.timestamp, lst))
          else
              RawLogEntry(x.event, x.entityType, x.timestamp, x.productID, x.listProduct, x.algorithm)              
        })
     }
     def groupByAction(lst: List[(String, Long, String, String)]) : List[(String, Int)] = {
      lst.groupBy(_._1).map(x => (x._1, x._2.size)).toList
     }
     def groupByAlgorithm(lst: List[RawLogEntry]) : List[(String, List[(String, Long, String, String)])] = {
        lst.groupBy(_.algorithm).toList.map(x => {
        val a= (x._1, x._2.map( y => {
              if("REC" == y.event){
                for{
                  item <- y.listProduct
                  val k = (y.event, y.timestamp, item.toString, y.algorithm)
                } yield k
              }else List((y.event, y.timestamp, y.productID.toString, y.algorithm))
            }).flatten
          )
        a
        })
     }
     def mergeSessions(lst: List[(String, List[(String, List[(String, Int)])])]): Map[String, Map[String, Int]] = {
      val x = lst.map(x => x._2)
      var y = x.flatten.groupBy(_._1).map(x => {
        (x._1, x._2.map(y => {
          y._2.map(z =>{
            (z._1, z._2)
            })
          }).flatten.groupBy(_._1).map(x => {
            (x._1,x._2.map( y =>{
              y._2
              }).sum)
            })
          )
        })
      y
     }
	def main(args: Array[String]){
    val sc = new SparkContext("local", "Rec count", "/opt/spark", List("target/scala-2.10/simple-project_2.10-1.0.jar"))

		val sm = ConnectDB.loadDB(sc)
       val r1 = groupBySession(sm)
       val r2 = r1.map(x => (x._1, assignRelativeAction(x._2)))
       val r3 = r2.map(x => (x._1, groupByAlgorithm(x._2)))
       val r4 = r3.map(x => (x._1, x._2.map(y => (y._1, groupByAction(y._2)))))
       val r5 = mergeSessions(r4)
       r5.foreach(println)
      sc.stop
	}
}
