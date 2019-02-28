package brandesSpark

import scala.io.Source
import scala.collection.mutable.{ArrayBuffer, Stack, Queue, Map}
//import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.rdd._
import org.apache.spark.broadcast._
import java.io.Serializable

//@SerialVersionUID(104L)
class GraphMetricBCSpark(val graph : brandesScala.Graph) extends Serializable{
  
  /*
 private def  initialization( node : Long): (Stack[Long],Map[Long, ArrayBuffer[Long]], Map[Long, Double],Map[Long, Double], Queue[Long] ) = {
    
     var S = Stack[Long]() 
    var PMap = scala.collection.mutable.Map(nodesRDD.map(x => (x,ArrayBuffer[Long]())).collect().toMap.toSeq: _*)
    var sigmaMap = scala.collection.mutable.Map(nodesRDD.map(x => (x,0.0)).collect().toMap.toSeq: _*)
    sigmaMap(node) = 1
    var distanceMap = scala.collection.mutable.Map(nodesRDD.map(x => (x,-1.0)).collect().toMap.toSeq: _*)
    distanceMap(node) = 0
    var Q = new Queue[Long]()
    Q += node
     (S, PMap, sigmaMap, distanceMap, Q)
  }
  
  
  private def computeTraversalStep(node : Long, S: Stack[Long], PMap : Map[Long, ArrayBuffer[Long]]
                         , sigmaMap: Map[Long, Double], distanceMap: Map[Long, Double], Q: Queue[Long]) {
    while (Q.size != 0){
      var v = Q.dequeue()
      S.push(v)
      var neighboors = graph.getNeighboors(v) 
      for (w <- neighboors.value) {
       
        if (distanceMap(w._1) < 0) { 
          Q.enqueue(w._1)            
          distanceMap(w._1) = distanceMap(v) + w._2
        }
        
        if (distanceMap(w._1) == distanceMap(v) + w._2) {
          sigmaMap(w._1) += sigmaMap(v)
          PMap(w._1) += v;
        }
      }
    }
  }
  
private def computeAccumulationStep(node : Long, S : Stack[Long], sigmaMap :  Map[Long, Double], 
      PMap : Map[Long,ArrayBuffer[Long]]) : Map[Long,Double] = {
       var deltaMap = scala.collection.mutable.Map(nodesRDD.map(x => (x,0.0)).collect().toMap.toSeq: _*)
       
    while (!S.isEmpty){
      var w = S.pop   
      for (v <- PMap(w)){
        deltaMap(v) += sigmaMap(v)/sigmaMap(w)*(1+deltaMap(w))}
    }
deltaMap(node) = 0.0
    deltaMap//.map(x => x._2)
  }
  
  
  def computeBC(node : Long) : Map[Long,Double] ={
    //println("start: "+ node)
    var (stack, pMap, sigmaMap, distanceMap, queue) = initialization(node)
    computeTraversalStep(node, stack, pMap, sigmaMap, distanceMap, queue)
    var delta = computeAccumulationStep(node, stack, sigmaMap, pMap)
    //println("end: "+node)
    delta
  }
  
  def computeDouble(node :Long) : Double ={
    node*2
  }
  /*val betweenessCentrality = graph.nodesRDD
                                   .map(x =>  computeDouble(x)).collect()*/
  */
}