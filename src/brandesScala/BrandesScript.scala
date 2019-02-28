package brandesScala

import scala.io.Source
import org.apache.log4j._
import scala.collection.mutable.{ArrayBuffer, Stack, Queue, Map}


object BrandesScript {
  def main(args : Array[String]){
    //Time
    val startTime = System.currentTimeMillis()
    //File extraction
    //val pathFileName = "../libimseti_random_50000_wc.csv"; val sep = ","
    val pathFileName = "../test_10_graph.csv"; val sep = " "
    val isDirected = false
    val  smallGraph = Source.fromFile(pathFileName)
        .getLines().map(row => {
      val tokens = row.split(sep).map(_.trim())
      tokens.length match {
        case 2 => {  (tokens(0).toLong, tokens(1).toLong, 1.0) }
        case 3 => { (tokens(0).toLong, tokens(1).toLong, tokens(2).toDouble) }
        case _ => { throw new IllegalArgumentException("invalid input line: " + row) }
      }}).toArray
    println(smallGraph.take(10).deep)
    
    //Garph params
    val nodes = smallGraph.map(x => x._1).toSet.union(smallGraph.map(x => x._2).toSet).toArray
    //val edges = edgeAll(10)
    
    val N = nodes.size
    println("There are "+ N + " edges")
    
    var bcMap = scala.collection.mutable.Map(nodes.map(x => (x, 0.0)).toMap.toSeq: _*)
    //var bcMap = scala.collection.mutable.Map(newbc.toSeq: _*)

    for (node <- nodes){
      //println("loop begins")
      //var node = nodes(n)
      
      var S = Stack[Long]() 
     
      var PMap = scala.collection.mutable.Map(nodes.map(x => (x,ArrayBuffer[Long]())).toMap.toSeq: _*)
      
      var sigmaMap = scala.collection.mutable.Map(nodes.map(x => (x,0.0)).toMap.toSeq: _*)
      sigmaMap(node) = 1
     
      var distanceMap = scala.collection.mutable.Map(nodes.map(x => (x,-1.0)).toMap.toSeq: _*)
      distanceMap(node) = 0
      
      
      var Q = new Queue[Long]()
      Q += node
      //println("Q : "+ Q)
      while (Q.size != 0){
        var v = Q.dequeue()
        S.push(v)
        
        var neighboors = getNeighboors(v, isDirected, smallGraph)     

        for (w <- neighboors) {
           if (distanceMap(w._1) < 0){
            Q.enqueue(w._1)            
            distanceMap(w._1) = distanceMap(v) + w._2
            
            }
          
          if (distanceMap(w._1) == distanceMap(v) + w._2) {
            sigmaMap(w._1 ) += sigmaMap(v)
            PMap(w._1) += v;
            }
           
        }
        
      }
        
    var delta = (for (i<- 1 to N) yield 0.0).toArray
    var deltaM = (nodes zip delta).toMap
    var deltaMap = scala.collection.mutable.Map(deltaM.toSeq: _*)
    
    while (!S.isEmpty){
      var w = S.pop
   
      for (v <- PMap(w)){
        deltaMap(v) += sigmaMap(v)/sigmaMap(w)*(1+deltaMap(w))
        
      }
     
      if (w!= node){
        
        bcMap(w) += deltaMap(w)//((N-2)*(N-1))
        
      }
      
    }
    println(deltaMap)
    }
    //bcMap.map(x => (x._1, x._2/()))
    val t1 = System.currentTimeMillis() - startTime;
    println("Time of computation: " + t1)
println(bcMap)
  }
  
  def getNeighboors(node : Long, isDirected : Boolean, graph: Array[(Long,Long, Double)]) : Array[(Long, Double)] = {
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

}