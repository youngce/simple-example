import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by mark on 11/7/15.
  */


object Stateful extends App{

  def createCkpoint()={
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(10))

    ssc.checkpoint("checkpoint")
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    ssc.sparkContext.parallelize(1 to 100)
    val pairs=words.map(word=>(word,1))

    def updateFunc(current:Seq[Int],previous:Option[Int])={
      val newCount=current.sum+previous.getOrElse(0)
      Some(newCount)
    }
    val wordCount=pairs.updateStateByKey(updateFunc _)

    wordCount.print()
    ssc
  }


  val ssc=StreamingContext.getOrCreate("checkpoint",createCkpoint)




  ssc.start()
  ssc.awaitTermination()

}
