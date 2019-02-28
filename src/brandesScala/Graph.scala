package brandesScala

import scala.io.Source
import scala.collection.mutable.{ArrayBuffer, Stack, Queue, Map}


class Graph(private val fileName: String,val path : String, val sep : String, val fileType : String, val isDirected : Boolean, val isWeighted : Boolean
            ) extends Serializable {
  
    
    val graph = Source.fromFile(path + fileName)
                      .getLines()
                      .map(row => {
                            val tokens = row.split(sep).map(_.trim())
                            tokens.length match {
                              case 2 => {  (tokens(0).toLong, tokens(1).toLong, 1.0)  }
                              case 3 => { (tokens(0).toLong, tokens(1).toLong, tokens(2).toDouble) }
                              case _ => { throw new IllegalArgumentException("invalid input line: " + row) }
                            }
                      })
                      .toArray

  
  val nodes = graph.map(x => x._1).toSet.union(graph.map(x => x._2).toSet).toArray
  
  
   
  val numberOfNodes = nodes.size
  
  def getInfo(){
      if (numberOfNodes < 15){
        println("Graph of : "+numberOfNodes + " nodes and : "+ graph.length + " edges\n" + graph.deep)
      }
      else {println("Graph of size: "+numberOfNodes+ " nodes and : "+ graph.length + " edges")}
  }
  

  def getNeighboors(node : Long) : Array[(Long, Double)] = {
    //handle doublons
    if (isDirected){
          graph.filter(x => (x._1 == node)).map(x => (x._2, x._3))
        }
    else {
          graph.filter(x => (x._1 == node) || (x._2 == node)).map(x => {
            if (x._1 == node) {(x._2, x._3)}
            else {(x._1 , x._3)}
            })
        }
  }
  
  def getNeighboorsMap(): Array[(Long,Array[(Long,Double)])] = {
    val nodes = getNodes()
    val neighboorsMap = nodes.map(x => (x,getNeighboors(x)))
    neighboorsMap
  }
  
  def getGraph() : Array[(Long,Long,Double)] = {
    graph
  }
  
  def getIsDirected(): Boolean = {
    isDirected
  }
  
  def getIsWeighted(): Boolean = {
    isWeighted
  }
 def getNodes() : Array[Long] = {
   nodes
 }
}

