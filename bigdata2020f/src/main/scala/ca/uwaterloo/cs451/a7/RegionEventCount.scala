/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package ca.uwaterloo.cs451.a7

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ManualClockWrapper, Minutes, StreamingContext}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.util.LongAccumulator
import org.rogach.scallop._

import scala.collection.mutable

class RegionEventCountConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, checkpoint, output)
  val input = opt[String](descr = "input path", required = true)
  val checkpoint = opt[String](descr = "checkpoint path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object RegionEventCount {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new RegionEventCountConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("EventCount")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 24)
    ssc.addStreamingListener(batchListener)
   
    val up_left_long : Double = -74.0141012
    val up_left_lat : Double = 40.7152191
    val up_right_long : Double = -74.013777
    val up_right_lat : Double = 40.7152275
    val down_right_long : Double = -74.0141027
    val down_right_lat : Double = 40.7138745
    val down_left_long : Double = -74.0144185
    val down_left_lat : Double = 40.7140753
    
      //citigroup
      
    val citi_up_left_long : Double = -74.011869
    val citi_up_left_lat : Double = 40.7217236
    val citi_up_right_long : Double = -74.009867
    val citi_up_right_lat : Double = 40.721493
    val citi_down_right_long : Double = -74.010140
    val citi_down_right_lat : Double = 40.720053
    val citi_down_left_long : Double =  -74.012083
    val citi_down_left_lat : Double =  40.720267    
    
   // val long=p(10).toDouble
   // val lat=p(11).toDouble
    //if(long>=up_left_long && lat<=up_left_lat || long<=up_right_long && lat<=up_right_lat||long<=down_right_long && lat>=down_right_lat||long>=down_left_long && lat>=down_left_lat)    


    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)
   // val goldman = [[-74.0141012, 40.7152191], [-74.013777, 40.7152275], [-74.0141027, 40.7138745], [-74.0144185, 40.7140753]]
    //val citigroup = [[-74.011869, 40.7217236], [-74.009867, 40.721493], [-74.010140,40.720053], [-74.012083, 40.720267]]
    val wc = stream.map(_.split(","))
//    val long=wc(6) 
      .map(p=>
      {
   val pick_up_longi=p(8).toDouble
   val pick_up_lat=p(9).toDouble
       val longi=p(10).toDouble
      val lat=p(11).toDouble
      var result="else"
   if(p(0)=="green")
     {
     if(pick_up_longi<List(up_left_long,up_right_long,down_left_long,down_right_long).max && pick_up_longi>List(up_left_long,up_right_long,down_left_long,down_right_long).min && pick_up_lat<List(up_left_long,up_right_long,down_left_long,down_right_long).max && pick_up_lat>List(up_left_long,up_right_long,down_left_long,down_right_long).min)
      {
      result="goldman"
      }
     else if(pick_up_longi<List(citi_up_left_long,citi_up_right_long,citi_down_left_long,citi_down_right_long).max && pick_up_longi>List(citi_up_left_long,citi_up_right_long,citi_down_left_long,citi_down_right_long).min && pick_up_lat<List(citi_up_left_long,citi_up_right_long,citi_down_left_long,citi_down_right_long).max && pick_up_lat>List(citi_up_left_long,citi_up_right_long,citi_down_left_long,citi_down_right_long).min)
      {
      result="citigroup"
      }
else
{
result="else"
}
      (result,1)
      }

else 
{
      if(longi<List(up_left_long,up_right_long,down_left_long,down_right_long).max && longi>List(up_left_long,up_right_long,down_left_long,down_right_long).min && lat<List(up_left_long,up_right_long,down_left_long,down_right_long).max && lat>List(up_left_long,up_right_long,down_left_long,down_right_long).min)
     
     {
      result="goldman"
      }
      else if(longi<List(citi_up_left_long,citi_up_right_long,citi_down_left_long,citi_down_right_long).max && longi>List(citi_up_left_long,citi_up_right_long,citi_down_left_long,citi_down_right_long).min && lat<List(citi_up_left_long,citi_up_right_long,citi_down_left_long,citi_down_right_long).max && lat>List(citi_up_left_long,citi_up_right_long,citi_down_left_long,citi_down_right_long).min)
      
      {
      result="citigroup"
      }
else
{
result="else"
}
      (result,1)

}
      }).filter(p=>p._1!="else")
     .reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y, Minutes(60), Minutes(60))
      .persist()

    wc.saveAsTextFiles(args.output())

    wc.foreachRDD(rdd => {
      numCompletedRDDs.add(1L)
    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}
