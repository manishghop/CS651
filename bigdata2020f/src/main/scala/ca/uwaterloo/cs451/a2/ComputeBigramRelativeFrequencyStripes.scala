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

package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner


class BigramStripesPMIConf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold", required = false, default = Some(10))
  val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
  val executorCores = opt[Int](descr = "number of cores", required = false, default = Some(1))
  verify()
}


object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new BigramStripesPMIConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("ComputeBigramRelativeFrequencyStripes")
    val sc = new SparkContext(conf)
    val red=args.reducers()
    val threshold=args.threshold()
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

  val textFile = sc.textFile(args.input(), args.reducers())
    textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) {
	
          tokens.sliding(2).map(p => {(p.head,Map(p.last->1.0))
	})
          // val bigramMarginal = tokens.init.map(w => (w, "*")).toList
          // bigram ++ bigramMarginal
        } else List()
      }).reduceByKey((a,b)=>{a++b.map{case (k,v) => k-> (v+a.getOrElse(k,0.0))}},red).map(stripe=>{ val sum=stripe._2.foldLeft(0.0)(_+_._2)  
 (stripe._1,stripe._2 map {case (k,v)=>  (k,(v/sum))    } ) }).map(s=>(s._1, s._2.toMap ))
.saveAsTextFile(args.output()) 
//val t=textFile.reduceByKey((a,b)=>{a++b}.map((k,v)=>{ case (k,v) k->                  )
//t.saveAsTextFile("data-check")
/*
      .map(bigram => (bigram, 1))
      .reduceByKey(_ + _)
      .sortByKey().groupByKey()
      .repartitionAndSortWithinPartitions(new MyPartitioner(args.reducers()))
      .mapPartitions(tmp => {
        var marginal = 0.0
        tmp.map(bi => {
          bi._1 match {
            case (_, "*") => {
              marginal = bi._2
              (bi._1, bi._2)
            }
            case (_, _) => (bi._1, bi._2 / marginal)
          }
        })
      })
      .map(p => "(" + p._1._1 + ", " + p._1._2 + "), " + p._2).saveAsTextFile(args.output())  */
  }
}
