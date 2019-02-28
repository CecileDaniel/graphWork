package brandesSpark
import java.io.Serializable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import org.apache.log4j.{Level,BasicConfigurator,Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Calendar



object BrandesSparkTest extends Serializable {
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    //////// PARAMETERS OF INPUT ////////
    val fileName = "random_1000_ud_uw.csv"
    val path =  "../"
    val delimiter = ","
    val fileType = "regularType"
    val isDirected = false
    val isWeighted = false
    ///////////////////////////////////
   /*BasicConfigurator.configure();
   val logger = Logger.getLogger("Graph Metrics with Spark")
   logger.setLevel(Level.INFO)
   val now = Calendar.getInstance().getTime()
   logger.info("start time : "+ now)*/
   

    ////////////// PARAMETERS OF SPARK CONTEXT //////
    
    val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("BrandesBasedBetwenness") 
    .set("spark.executor.cores","2")
    .set("spark.driver.memory", "4g")
    .set("spark.executor.memory", "4g")
    .set("deploy-mode","cluster")
    .set("spark.network.timeout", "100000000")
    .set("spark.driver.maxResultSize", "50g")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //----> http://spark.apache.org/docs/latest/tuning.html#data-serialization
    .set("spark.kryoserializer.buffer.max", "128m")
    .set("spark.kryoserializer.buffer", "64m")
    .registerKryoClasses(Array(classOf[ArrayBuffer[String]], classOf[ListBuffer[String]]))
    /////////////////////////////////////////
    val sc = new SparkContext(conf)
    
   val smallGraph = new brandesScala.Graph(fileName,path, delimiter, fileType, isDirected, isWeighted)
   val nodes = smallGraph.getNodes() 
   val startTime = System.currentTimeMillis
   println("nb of nodes: "+ smallGraph.numberOfNodes)
   val metric = new brandesScala.GraphMetrics(smallGraph) 
   
   val neighboorsMap = smallGraph.getNeighboorsMap().toMap
   val nodesRDD = sc.parallelize(nodes)    
   val betweennessCentrality = new brandesScala.ParGraphMetrics(smallGraph ).getBC(sc)
   println(betweennessCentrality.take(3))
   val endTime = System.currentTimeMillis
   println("duration: "+ (endTime -startTime)/1000.0+"sec")
   sc.stop()
  }
}