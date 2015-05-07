package com.test

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.StreamingContext._

import scala.collection._
import scala.collection.mutable._


/**
 * Created by Honda on 2015/4/24.
 */

object KafkaStreamApp
{
    def main(args: Array[String])
    {
        if (args.length < 4)
        {
            println("Usage <zookeeper> <group-id> <topics> <numTreds>")
            System.exit(1);
        }

        val Array(zookeeper, groupId, topics, numThreads) = args
        val sparkConf = new SparkConf().setAppName("KafkaStreamApp")
        val ssc = new StreamingContext(sparkConf, Seconds(2))
        val fileChk = new Thread(new FileChecker(ssc))
        fileChk.start()
        val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
        val lines = KafkaUtils.createStream(ssc, zookeeper, groupId, topicMap).map(_._2)
        println("===================Start=====================")

        lines.print()
        val words = lines.flatMap(_.split(" "))
        val wordCount = words.map(x => (x, 1)).reduceByKey(_ + _)
        wordCount.print()
        println("===================End=====================")
        val arrBuf = new ArrayBuffer[String]()

        println("start:" + arrBuf.size)
        wordCount.foreachRDD(rdd =>
        {
            rdd.foreach(rddRec =>
            {
                val key = rddRec._1
                println("key:" + key)
                val countResult = rddRec._2
                println("value:" + countResult)
                arrBuf.append(key + "," + countResult)
                println("Inner:" + arrBuf.size)

            })

            println("outer:" + arrBuf.size)
            if (arrBuf.size > 0)
            {
                val outData = ssc.sparkContext.parallelize(arrBuf)
                outData.coalesce(1).saveAsTextFile("file:///home/hduser/mytest.txt")
                println("here")
            }

        })
        println("before ssc:" + arrBuf.size )

        ssc.start()
        ssc.awaitTermination()

    }

}
