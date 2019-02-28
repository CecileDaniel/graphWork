package brandesScala

import scala.io.Source
import scala.collection.mutable.{ArrayBuffer, Stack, Queue, Map}
import java.io.Serializable
import org.apache.spark.SparkContext

class GraphMetrics(val graph : Graph) extends Serializable {
  val nodes = graph.getNodes()
  


  private def initialization(node : Long): (Stack[Long],Map[Long, ArrayBuffer[Long]], Map[Long, Double],Map[Long, Double], Queue[Long] ) = {
        //initialization 
    var S = Stack[Long]() 
    var PMap = scala.collection.mutable.Map(nodes.map(x => (x,ArrayBuffer[Long]())).toMap.toSeq: _*)
    var sigmaMap = scala.collection.mutable.Map(nodes.map(x => (x,0.0)).toMap.toSeq: _*)
    sigmaMap(node) = 1
    var distanceMap = scala.collection.mutable.Map(nodes.map(x => (x,-1.0)).toMap.toSeq: _*)
    distanceMap(node) = 0
    var Q = new Queue[Long]()
    Q += node
    
    (S, PMap, sigmaMap, distanceMap, Q)
  }
  
  def getSSSPFromNode(node : Long) : (Stack[Long], Map[Long, Double], Map[Long, ArrayBuffer[Long]]) = {
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
  }
  
 def getDelta(node : Long) : Map[Long,Double] = {
   var (stack, sigmaMap,pMap) = getSSSPFromNode(node)
       var deltaMap = scala.collection.mutable.Map(nodes.map(x => (x,0.0)).toMap.toSeq: _*)
       
    while (!stack.isEmpty){
      var w = stack.pop   
      for (v <- pMap(w)){
        deltaMap(v) += sigmaMap(v)/sigmaMap(w)*(1+deltaMap(w))}
    }
   deltaMap(node) = 0.0
   deltaMap
  }
  

  

  
  def computeDouble(node : Long) : Double = {
    node*2
  }
}