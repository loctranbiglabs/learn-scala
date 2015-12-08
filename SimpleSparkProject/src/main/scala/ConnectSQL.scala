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
case class OtherData(timestamp: Long, sessionId: String,action: String, product:Product, algorithm: String)
case class RecData(timestamp: Long, sessionId: String,action: String, listProduct:List[String], algorithm: String)

case class Result(productID: String, totalView: Int,totalRec: Int)


object RecCount {
     def minTs(lst: List[(String, Long)]) :Long = {
       val r = lst.reduce((x,y) => {
            if(x._1 == "REC" && x._2 < y._2){
              x
            }else if(y._1 == "REC" && y._2 < x._2){
              y
            }else ("REC",999999999L)
          }
        )
        r._2
     }
	def main(args: Array[String]){
		val sm = ConnectDB.loadDB()

  //       val rst1 = sm.groupBy( _._1 ).map( kv => (kv._1, kv._2.map( x=> {
  //           if(x._2.listProduct.isEmpty) List((x._2.productID,x._2.event, x._2.timestamp))
  //           else
  //           for{
  //               item <- x._2.listProduct
  //               val k = (item, x._2.event, x._2.timestamp)
  //             } yield k

  //         }).flatten.groupBy(_._1).map(k =>(k._1, k._2.map(x => {
  //             if((x._2 =="REC") || (x._2 != "REC" && x._3 >= minTs(k._2.map(k=> (k._2, k._3))))) x._2
  //             else "0"
  //           }))))).map(x => {(x._1, x._2.filter(
  //             x=> x._2.size > 1 && x._2.indexOf("REC") > -1
  //           ).map(z=>{
  //             z._1 -> z._2.map(x=>(x,1)).groupBy(_._1).map(t => (t._1, t._2.size))
  //             }))}).map(x => x._2).flatten.groupBy(_._1).map(x =>{
  //             (x._1, x._2.groupBy(_._1).map( y => y._2).flatten.map( z => 
  //               z._2).flatten.groupBy(_._1).map( t =>{
  //                 (t._1, t._2.map(x => x._2).sum)
  //                 }))
  //             })

		// rst1.foreach(println)

        /*val rst2 = sm.groupBy( _._1 ).map( kv => (kv._1, kv._2.map( x=> {
            (x._2.algorithm,x._2.event, x._2.timestamp)
            
          }).groupBy(_._1).map(k =>(k._1, k._2.map(x => {
              x._2
          })))))
        .map(x => {(x._1, x._2.map(z=>{
              z._1 -> z._2.map(x=>(x,1)).groupBy(_._1).map(t => (t._1, t._2.size))
              }))}).map(x => x._2).flatten.groupBy(_._1).map(x =>{
              (x._1, x._2.groupBy(_._1).map( y => y._2).flatten.map( z => 
                z._2).flatten.groupBy(_._1).map( t =>{
                  (t._1, t._2.map(x => x._2).sum)
                  }))
              })

		rst2.foreach(println)*/
sm.foreach(println)

	}
}
