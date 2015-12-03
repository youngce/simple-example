import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by mark on 12/3/15.
  */
object Output extends App{
  val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
  val ssc = new StreamingContext(conf, Seconds(10))
  val lines = ssc.socketTextStream("localhost", 9999)
  val words = lines.flatMap(_.split(" "))


  words.foreachRDD { rdd =>

    // Get the singleton instance of SQLContext
    val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
    import sqlContext.implicits._

    // Convert RDD[String] to DataFrame
    val wordsDataFrame = rdd.toDF("word")

    // Register as table
    wordsDataFrame.registerTempTable("words")

    // Do word count on DataFrame using SQL and print it
    val wordCountsDataFrame =
      sqlContext.sql("select word, count(*) as total from words group by word")
    wordCountsDataFrame.show()
  }
  ssc.start()
  ssc.awaitTermination()

}
