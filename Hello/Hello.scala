object HelloWorld {

    case class Properties(data: String)
    case class Product(productID: Int)
    case class Data(timestamp: Long, sessionId: String,action: String)
    case class OtherData(timestamp: Long, sessionId: String,action: String, product:Product)
    case class RecData(timestamp: Long, sessionId: String,action: String, listProduct:List[Int])
    
    case class Result(productID: Int, totalView: Int,totalRec: Int)
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
        case class RawLogEntry(
          event: String,
          entityType: String,
          timestamp: Long,
          productID: Int,
          listProduct: List[Int],
          algorithm: String
      )
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
      def main(args: Array[String]) {

      val lst = List(
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("VIEW","GUEST",1449556227184L,10004,List(),"")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("REC","GUEST",1449556239864L,0,List(10004, 10005),"topview")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("REC","GUEST",1449556239864L,0,List(10004, 9952, 10005),"topviewall")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("VIEW","GUEST",1449556258989L,9952,List(),"topviewall")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("REC","GUEST",1449556260255L,0,List(9952),"topview")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("REC","GUEST",1449556260342L,0,List(10004, 9952, 10005),"topviewall")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("VIEW","USER",1449556297116L,9952,List(),"topviewall")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("REC","USER",1449556299893L,0,List(9952),"topview")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("REC","USER",1449556300097L,0,List(10004, 9952, 10005),"topviewall")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("ADD_WISHLIST","USER",1449556301079L,9952,List(),"")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("VIEW","USER",1449556320821L,10001,List(),"")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("REC","USER",1449556325011L,0,List(10004, 9952, 10005),"topviewall")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("ADD_WISHLIST","USER",1449556333161L,10001,List(),"")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("REC","USER",1449556341498L,0,List(10004, 10005),"topview")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("VIEW","USER",1449556339055L,10005,List(),"topviewall")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("REC","USER",1449556341636L,0,List(10004, 9952, 10005),"topviewall")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("REC","USER",1449556422953L,0,List(10004, 9952),"topviewall")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("VIEW","USER",1449556425701L,10004,List(),"topviewall")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("REC","USER",1449556426665L,0,List(10004),"topview")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("VIEW","USER",1449556418369L,10005,List(),"topviewall")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("REC","USER",1449556422945L,0,List(10004),"topview")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("REC","USER",1449556426843L,0,List(10004, 9952),"topviewall")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("VIEW","USER",1449556443455L,10004,List(),"topviewall")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("VIEW","USER",1449556446862L,10004,List(),"topviewall")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("REC","USER",1449556448972L,0,List(10004),"topview")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("REC","USER",1449556449117L,0,List(10004, 9952),"topviewall")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("VIEW","USER",1449556452219L,10003,List(),"crosssales")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("REC","USER",1449556453483L,0,List(10004),"topview")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("REC","USER",1449556453629L,0,List(10004, 9952),"topviewall")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("VIEW","USER",1449556507221L,10004,List(),"topview")),
            ("3yjqyv5ox0fre0rjx6eqabev",RawLogEntry("VIEW","USER",1449556507221L,10004,List(),"crosssales")),
            ("1mybh0a25lts1vji4ddnbx618",RawLogEntry("REC","GUEST",1449556942780L,0,List(10004, 10005, 9952, 10003, 10001),"topviewall")),
            ("1mybh0a25lts1vji4ddnbx618",RawLogEntry("VIEW","GUEST",1449556934239L,10021,List(),"")),
            ("1mybh0a25lts1vji4ddnbx618",RawLogEntry("REC","GUEST",1449556938689L,0,List(10004, 10005, 9952, 10003, 10001),"topviewall")),
            ("1mybh0a25lts1vji4ddnbx618",RawLogEntry("VIEW","GUEST",1449556941560L,10004,List(),"topviewall")),
            ("1mybh0a25lts1vji4ddnbx618",RawLogEntry("REC","GUEST",1449556942702L,0,List(10004, 10005, 10003),"topview"))
        )


       val r1 = groupBySession(lst)
       val r2 = r1.map(x => (x._1, assignRelativeAction(x._2)))
       val r3 = r2.map(x => (x._1, groupByAlgorithm(x._2)))
       val r4 = r3.map(x => (x._1, x._2.map(y => (y._1, groupByAction(y._2)))))
       val r5 = mergeSessions(r4)
       r5.foreach(println)

/*        val rst = lst.groupBy( _._1 ).map( kv => (kv._1, kv._2.map( x=> {
            if(x._2.listProduct.isEmpty) List((x._2.productID,x._2.event, x._2.timestamp))
            else
            for{
                item <- x._2.listProduct
                val k = (item, x._2.event, x._2.timestamp)
              } yield k

          }).flatten.groupBy(_._1).map(k =>(k._1, k._2.map(x => {
              if((x._2 =="REC") || (x._2 != "REC" && x._3 >= minTs(k._2.map(k=> (k._2, k._3))))) x._2
              else "0"
            }))))).map(x => {(x._1, x._2.filter(
              x=> x._2.size > 1 && x._2.indexOf("REC") > -1
            ).map(z=>{
              z._1 -> z._2.map(x=>(x,1)).groupBy(_._1).map(t => (t._1, t._2.size))
              }))}).map(x => x._2).flatten.groupBy(_._1).map(x =>{
              (x._1, x._2.groupBy(_._1).map( y => y._2).flatten.map( z => 
                z._2).flatten.groupBy(_._1).map( t =>{
                  (t._1, t._2.map(x => x._2).sum)
                  }))
              })
        rst.foreach(println)*/
     }
}


