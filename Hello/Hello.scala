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
      def main(args: Array[String]) {
          case class RawLogEntry(
          	event: String,
          	entityType: String,
          	timestamp: Long,
          	productID: Int,
          	listProduct: List[Int]
      	)
      val lst = List(("8cv5o85exdoi7b22jxjyzykr",RawLogEntry("VIEW","GUEST",65464863,10005,List())),
        ("8cv5o85exdoi7b22jxjyzykr",RawLogEntry("VIEW","GUEST",65791353,10007,List())),
        ("8cv5o85exdoi7b22jxjyzykr",RawLogEntry("VIEW","GUEST",65806194,10003,List())),
        ("8cv5o85exdoi7b22jxjyzykr",RawLogEntry("VIEW","GUEST",65809183,10005,List())),
        ("8cv5o85exdoi7b22jxjyzykr",RawLogEntry("VIEW","GUEST",65966921,10004,List())),
        ("8cv5o85exdoi7b22jxjyzykr",RawLogEntry("VIEW","GUEST",66211931,10004,List())),
        ("8cv5o85exdoi7b22jxjyzykr",RawLogEntry("VIEW","GUEST",66231838,10004,List())),
        ("8cv5o85exdoi7b22jxjyzykr",RawLogEntry("VIEW","GUEST",66257151,10004,List())),
        ("8cv5o85exdoi7b22jxjyzykr",RawLogEntry("VIEW","GUEST",66552338,10004,List())),
        ("8cv5o85exdoi7b22jxjyzykr",RawLogEntry("VIEW","GUEST",66565848,10007,List())),
        ("14411ycunamxvqg90he7y0ntk",RawLogEntry("VIEW","GUEST",66689777,10013,List())),
        ("14411ycunamxvqg90he7y0ntk",RawLogEntry("VIEW","GUEST",66692688,9950,List())),
        ("14411ycunamxvqg90he7y0ntk",RawLogEntry("VIEW","GUEST",66695259,10004,List())),
        ("14411ycunamxvqg90he7y0ntk",RawLogEntry("VIEW","GUEST",66697642,10010,List())),
        ("14411ycunamxvqg90he7y0ntk",RawLogEntry("VIEW","GUEST",66702908,9951,List())),
        ("1grhkkdftd8wdtibou9m2c97p",RawLogEntry("VIEW","GUEST",67183241,10035,List())),
        ("1grhkkdftd8wdtibou9m2c97p",RawLogEntry("VIEW","GUEST",67301223,10035,List())),
        ("1grhkkdftd8wdtibou9m2c97p",RawLogEntry("VIEW","GUEST",67308431,10035,List())),
        ("1grhkkdftd8wdtibou9m2c97p",RawLogEntry("VIEW","GUEST",67314414,10004,List())),
        ("1ls4end5iay22a2b6rf5fl4m4",RawLogEntry("VIEW","GUEST",67897201,9950,List())),
        ("1ls4end5iay22a2b6rf5fl4m4",RawLogEntry("VIEW","GUEST",67907735,9950,List())),
        ("1ls4end5iay22a2b6rf5fl4m4",RawLogEntry("VIEW","GUEST",67912239,9950,List())),
        ("a4egocu0gfrenio8mjkbjks9",RawLogEntry("VIEW","GUEST",69698417,10003,List())),
        ("1h3cair5b22zt1bw4vtu6alwcb",RawLogEntry("VIEW","GUEST",69726542,10004,List())),
        ("1h3cair5b22zt1bw4vtu6alwcb",RawLogEntry("VIEW","GUEST",69831460,10007,List())),
        ("wfw2sqac0ziwinb557secbi7",RawLogEntry("VIEW","GUEST",75281640,9950,List())),
        ("wfw2sqac0ziwinb557secbi7",RawLogEntry("VIEW","GUEST",75287014,10007,List())),
        ("wfw2sqac0ziwinb557secbi7",RawLogEntry("VIEW","GUEST",75358940,10007,List())),
        ("1181k1vhsk1bg6u8bumyiefvb",RawLogEntry("VIEW","GUEST",75610357,10007,List())),
        ("1f5rrorze7ejdsp6btjt4v2oy",RawLogEntry("VIEW","GUEST",76193840,10007,List())),
        ("fscwbh0rlocfwrkn2kvp8wlz",RawLogEntry("VIEW","GUEST",77263667,1234,List())),
        ("fscwbh0rlocfwrkn2kvp8wlz",RawLogEntry("ADD_CARD","GUEST",77392965,10007,List())),
        ("fscwbh0rlocfwrkn2kvp8wlz",RawLogEntry("VIEW","GUEST",77477058,10002,List())),
        ("fscwbh0rlocfwrkn2kvp8wlz",RawLogEntry("VIEW","GUEST",77481565,9952,List())),
        ("fscwbh0rlocfwrkn2kvp8wlz",RawLogEntry("VIEW","GUEST",77484391,10013,List())),
        ("fscwbh0rlocfwrkn2kvp8wlz",RawLogEntry("VIEW","GUEST",77487264,9950,List())),
        ("fscwbh0rlocfwrkn2kvp8wlz",RawLogEntry("VIEW","GUEST",77490324,10004,List())),
        ("fscwbh0rlocfwrkn2kvp8wlz",RawLogEntry("VIEW","GUEST",77491381,324,List())),
        ("fscwbh0rlocfwrkn2kvp8wlz",RawLogEntry("VIEW","GUEST",77494205,10007,List())),
        ("fscwbh0rlocfwrkn2kvp8wlz",RawLogEntry("VIEW","GUEST",77495076,2345,List())),
        ("czezu6i0u1241get692r3aesz",RawLogEntry("VIEW","GUEST",77718630,10007,List())),
        ("czezu6i0u1241get692r3aesz",RawLogEntry("REC","GUEST",77723653,234,List(10003, 10005, 10004))),
        ("czezu6i0u1241get692r3aesz",RawLogEntry("VIEW","GUEST",77729942,10005,List())),
        ("czezu6i0u1241get692r3aesz",RawLogEntry("REC","GUEST",77731305,12312,List(10003, 10007, 10004)))
        )
        val rst = lst.groupBy( _._1 ).map( kv => (kv._1, kv._2.map( x=> {
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
            ))})

        // rst.foreach(println)

         // val m = for{
         //  item <- rst
         //  val res = item._2.filter(x => (x._2 !="REC") && (x._1 == 10007))
         // } yield res
         // m.foreach(println)
         rst.foreach(println)

        // val a = List(List(1003,1004), List(1005, 1006))
        // println(a.flatten)
     }
}


