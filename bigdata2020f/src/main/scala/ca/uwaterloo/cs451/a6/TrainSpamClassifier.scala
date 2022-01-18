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

class A6Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model,shuffle)
  val input = opt[String](descr = "input path", required = true)
  val model= opt[String](descr="output path",required=true)
   val shuffle= opt[Boolean](descr="want to shuffle or not",required=false)
  verify()
}

object TrainSpamClassifier extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new A6Conf(argv)

    log.info("Input: " + args.input())
    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    
    
    val textFile = sc.textFile(args.input())
if(args.shuffle()==true)
   {
    textFile.map(p=>(scala.util.Random.nextInt(),p)).sortByKey().map(p=>(p._2))
   }   
    // w is the weight vector (make sure the variable is within scope)
    val w = scala.collection.mutable.Map[Int, Double]()
    
    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]) : Double = {
      var score = 0d
      features.foreach(f => if (w.contains(f)) score += w(f))
      score
    }

    // This is the main learner:
    val delta = 0.002

    // For each instance...
    //val isSpam = 0   // label
    //val features = ... // feature vector of the training instance

    // Update the weights as follows:

    
     val exp= 2.71d
    val trained = textFile.map(line =>{
      val tokens=line.split(' ')
      val docid=tokens(0)
      val isSpam= if (tokens(1)=="spam") 1d else 0d
//      val features=tokens(2)
      val features=tokens.drop(2).map(_.toInt)
      (0, (docid, isSpam, features))
      }).groupByKey(1).flatMap(p=>{
      p._2.foreach(items=>{
      
      val features=items._3
      val isSpam=items._2
      val docid=items._1
      val score = spamminess(features)
      val prob = 1.0 / (1 + scala.math.exp(-score))
      features.foreach(f=>{

      if (w.contains(f)) {

        w(f) += (isSpam - prob) * delta
      } else {
        w(f) = (isSpam - prob) * delta
       }
      })

      })
      w
      })
      // Then run the trainer...

    trained.saveAsTextFile(args.model())

}
}


   





    
    




