package brandesSpark
import scala.io.Source
import org.apache.spark.{SparkConf, SparkContext, rdd, broadcast}
import java.io.Serializable

//@SerialVersionUID(100L)
class Graph(val sc: SparkContext, val fileName : String, val path : String, val sep :String,  val fileType : String ) extends java.io.Serializable {
  
  //val graphScala = new brandesScala.Graph(fileName  , path, sep, fileType)
  //val graphRDD = sc.parallelize(graphScala.graph)
   /*val graphRDD = sc.textFile(path+fileName).map(row => {
      val tokens = row.split(sep).map(_.trim())
      tokens.length match {
        case 2 => { (tokens(0).toLong, tokens(1).toLong, 1.0) }
        case 3 => { (tokens(0).toLong, tokens(1).toLong, tokens(2).toDouble) }
        case _ => { throw new IllegalArgumentException("invalid input line: " + row) }
      }
    })*/
    
  val graph = Source.fromFile(path + fileName)
                      .getLines()
                      .map(row => {
                            val tokens = row.split(sep).map(_.trim())
                            tokens.length match {
                              case 2 => {  (tokens(0).toLong, tokens(1).toLong, 1.0)  }
                              case 3 => { (tokens(0).toLong, tokens(1).toLong, tokens(2).toDouble) }
                              case _ => { throw new IllegalArgumentException("invalid input line: " + row) }
                            }
                      }).toArray
                      
  val graphRDD = sc.parallelize(graph)
    
                                            
  val nodesRDD = graphRDD.map(x => x._1).union(graphRDD.map(x => x._2)).distinct()
  val numberOfNodes = nodesRDD.count()
  
  val isDirected = false
  val isWeighted = getIsWeightedInfo //is it better to have a function or not
  val isWeightedDirectly = (graphRDD.map(x => x._3).max() == graphRDD.map(x => x._3).max()) && (graphRDD.map(x => x._3).max()==1.0)==false
  
  
  def getNeighboors(node : Long) :  broadcast.Broadcast[Array[(Long, Double)]] = {
    if (isDirected){
      val neigh = graphRDD.filter(x => x._1 ==node).map(x => (x._2, x._3))
                          .distinct()
                          .collect()
                          .toArray
      sc.broadcast(neigh)
    }
    else{
     val neigh = graphRDD.filter(x => (x._1 == node)||(x._2 == node)).map(x => if (x._1 == node) (x._2, x._3) else (x._1, x._3))
                         .distinct()
                         .collect()
                         .toArray
    sc.broadcast(neigh)
    }
  }
  
  def getIsWeightedInfo(): Boolean = {
    if ((graphRDD.map(x => x._3).max() == graphRDD.map(x => x._3).max()) && (graphRDD.map(x => x._3).max()==1.0)) {
      false
    }
    else true
  }

}