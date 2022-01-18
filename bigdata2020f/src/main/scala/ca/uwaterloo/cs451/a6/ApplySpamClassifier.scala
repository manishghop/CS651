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

class A6Conf1(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model)
  val input = opt[String](descr = "input path", required = true)
  val output= opt[String](descr = "output path", required=true)
  val model= opt[String](descr="output path",required=true)
  verify()
}

object ApplySpamClassifier extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new A6Conf1(argv)

    log.info("Input: " + args.input())
    val conf = new SparkConf().setAppName("ApplySpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)


    val textFile = sc.textFile(args.input())

    // w is the weight vector (make sure the variable is within scope)
    val w = scala.collection.mutable.Map[Int, Double]()
   val  mapper=sc.broadcast(sc.textFile(args.model()+"/part-00000").map(p=>{
    val tokens=p.substring(1,p.length()-1).split(',')
    (tokens(0).toInt,tokens(1).toDouble)
    }).collectAsMap())
    
    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]) : Double = {
      var score = 0d
      features.foreach(f => if (mapper.value.contains(f)) score += mapper.value(f))
      score
       }

    // This is the main learner:
    val delta = 0.002



   //  val exp= 2.71d
    val trained = textFile.map(line =>{
      val tokens=line.split(' ')
      val docid=tokens(0)
      val isSpam=tokens(1)
//      val features=tokens(2)
      val features=tokens.drop(2).map(_.toInt)
      val score = spamminess(features)
      //val prob = 1.0 / (1 + exp(-score))
      val result= if(score>0) "spam" else "ham"
      (docid,isSpam,score,result)
      })
      // Then run the trainer...

    trained.saveAsTextFile(args.output())

}
}

