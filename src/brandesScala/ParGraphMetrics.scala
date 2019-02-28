package brandesScala

import scala.io.Source
import scala.collection.mutable.{ArrayBuffer, Stack, Queue, Map}
import java.io.Serializable
import org.apache.spark.SparkContext

class ParGraphMetrics( graph : Graph)   extends GraphMetrics(graph) {
  
  /*override def getSSSPFromNode(node : Long) : (Stack[Long], Map[Long, Double], Map[Long, ArrayBuffer[Long]]) = {
    var  (stack, pMap, sigmaMap, distanceMap, queue) = initialization(node)
    
    
    while (queue.size != 0){
      var v = queue.dequeue()
      stack.push(v)
      
      var neighboors = graph.getNeighboors(v)     

      for (w <- neighboors) {
        if (distanceMap(w._1) < 0){
          queue.enqueue(w._1)            
          distanceMap(w._1) = distanceMap(v) + w._2
        }
        
        if (distanceMap(w._1) == distanceMap(v) + w._2) {
          sigmaMap(w._1) += sigmaMap(v)
          pMap(w._1) += v;
        }
      }
    }
    (stack, sigmaMap,pMap)
  }*/
  
  
  def getBC(sc :SparkContext, normalized : Boolean = false): scala.collection.immutable.Map[Long, Double] = {
    val isDirected = graph.getIsDirected()
    val nodesRDD = sc.parallelize(nodes)
    val coeff = (normalized, isDirected) match {
      case (true, false) => (nodes.length -1)*(nodes.length -2)
      case (true, true) => (nodes.length -1)*(nodes.length -2)
      case (false, false) => 2
      case (false, true) => 1
    }
    
    val betweennessCentrality = nodesRDD.map(x =>  getDelta(x))
                                       .map(x => x.toList)
                                       .reduce(List.concat(_, _))
                                       .groupBy(_._1).mapValues(seq => seq.map(_._2).reduce(_ + _))
                                       .map(x => (x._1, x._2/coeff))
  betweennessCentrality
  }

}