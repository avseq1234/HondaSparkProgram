package com.test



import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

/**
 * Created by Honda on 2015/4/22.
 */
object SparkStreamingApp
{

    def main(args: Array[String])
    {
        val conf = new SparkConf().setAppName("SparkStreamingApp")
        //val sc = new SparkContext
        val ssc = new StreamingContext(conf, Seconds(1))
        val lines = ssc.socketTextStream("localhost", 9999)
        val words = lines.flatMap(_.split(" "))
        val pairs = words.map((_, 1))

        val wordCounts = pairs.reduceByKey(_ + _)
        println("*************** Show Result *******************")
        wordCounts.print()
        println("*************** End Result *******************")
        ssc.start()
        ssc.awaitTermination()
    }
}
