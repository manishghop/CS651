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

package ca.uwaterloo.cs451.a6


import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession
import scala.math.exp

class Conf4(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model,method)
  val input = opt[String](descr = "input path", required = true)
  val output= opt[String](descr = "output path", required=true)
  val model= opt[String](descr="output path",required=true)
  val method =opt[String](descr="method",required=true)
  verify()
}

object ApplyEnsembleSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf4(argv)

    log.info("Input: " + args.input())
    val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
  
    val tested = sc.textFile(args.input())
   val method=args.method()
   val trainer1=sc.broadcast(sc.textFile(args.model()+"/part-00000").map(p=>{
    val tokens=p.substring(1,p.length()-1).split(",")
    (tokens(0).toInt,tokens(1).toDouble)
    }).collectAsMap())

    val trainer2=sc.broadcast(sc.textFile(args.model()+"/part-00001").map(p=>{
    val tokens=p.substring(1,p.length()-1).split(",")
    (tokens(0).toInt,tokens(1).toDouble)
    }).collectAsMap())

    val trainer3=sc.broadcast(sc.textFile(args.model()+"/part-00002").map(p=>{
    val tokens=p.substring(1,p.length()-1).split(",")
    (tokens(0).toInt,tokens(1).toDouble)
    }).collectAsMap())


    //print(trainer1)

    // Scores a document based on its list of features.
    def spamminess(features: Array[Int],weights:scala.collection.Map[Int,Double]) : Double = {
      var score = 0d
      features.foreach(f => if (weights.contains(f)) score += weights(f))
      score
       }


    // This is the main learner:
    val delta = 0.002



    val trained = tested.map(line =>{
      val tokens=line.split(' ')
      val docid=tokens(0)
      val isSpam=tokens(1)
       val features=tokens.drop(2).map(_.toInt)
       val train_1=spamminess(features,trainer1.value)
       val train_2= spamminess(features,trainer2.value)
       val train_3= spamminess(features,trainer3.value)
       var score=0d
if(method=="average")
{
score=(train_1+train_2+train_3)/3
}
else
{
val spam1=if(train_1>0) 1d else -1d
val spam2=if(train_2>0) 1d else -1d
val spam3=if(train_3>0) 1d else -1d
score=spam1+spam2+spam3
}
val result=if(score>0) "spam" else "ham"
      (docid,isSpam,score,result)
      })
trained.saveAsTextFile(args.output())


}
}


















