package com.test

import org.apache.spark._
import org.apache.spark.SparkContext._;

/**
 * Created by Honda on 2015/4/20.
 */
object FirstSpark
{
    def main(args: Array[String])
    {
        println("====================Start========================")
        val scConf = new SparkConf().setAppName("HondaWordCount")
        val sc = new SparkContext(scConf)
        val file = sc.textFile("myfile.txt").cache()
        println("========== Confirm Read File OK==================")
        println("Line:" + file.count())
        val counts = file.flatMap(_.split(" ")).map(w => (w, 1)).reduceByKey((a: Int, b: Int) => a +b)
        counts.saveAsTextFile("mycounts")

    }
}
