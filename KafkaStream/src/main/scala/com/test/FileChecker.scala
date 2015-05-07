package com.test
/**
 * Created by Honda on 2015/4/29.
 */

import org.apache.spark.streaming._

class FileChecker( ssc:StreamingContext ) extends Runnable
{
    def run(): Unit =
    {
        println("Start Run...")
        var check = true
        while(check)
        {

            val stopFile = ssc.sparkContext.textFile("stop.file")
            if( stopFile.count() > 0)
            {
                println("Stop check")
                check = false
                ssc.stop(true , true )

            }
            Thread.sleep(1000);
            println("keep Check....")
        }
    }
}
