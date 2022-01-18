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



class StripesPMIConf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold", required = false, default = Some(10))
  val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
  val executorCores = opt[Int](descr = "number of cores", required = false, default = Some(1))
  verify()
}




object StripesPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new StripesPMIConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("StripesPMI")
    val sc = new SparkContext(conf)
    val threshold=args.threshold()    
    val red=args.reducers()    
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val interDir=new Path("cs451-inter-pairspmi")
    FileSystem.get(sc.hadoopConfiguration).delete(interDir,true)
    val path="cs451-inter-pairspmi"


     var txt=sc.textFile(args.input())
    var total_lines=txt.count()
    var words=txt.flatMap(line=>{
    var tokens=tokenize(line)
    if(tokens.length>0)
    {
	//List()
	tokens.take(Math.min(tokens.length,40)).distinct
//   tokens.distinct
    }
   else List()
    
    
    
    }).map(p=>(p,1)).reduceByKey(_+_).collectAsMap()
    
    var broadcast_var=sc.broadcast(words)
    
    txt.flatMap(line=>{
    var tokens=tokenize(line)
    val words=tokens.take(Math.min(tokens.length,40)).distinct
    var iter=scala.collection.mutable.ListBuffer[(String,Map[String,Int])]()
//    var mapper=scala.collection.mutable.Map[String,Integer]()
    if(words.length>1)
    {
    var i=0
    var j=0
    for(i<- 0 to words.length-1)
    {
    var mapper=scala.collection.mutable.Map[String,Int]()  // for each ith word we create its hashmap
    for(j<- 0 to words.length-1)
    {
    if((i!=j) && (words(i)!=words(j)))
    {
    var value=mapper.getOrElse(words(j),0)+1
    mapper(words(j))=value
    }
    //var pair:(String,String)= (tokens(i),tokens(j))
    //mapper+=tokens(i)->tokens(j)
    }
    var temp:(String,Map[String,Int])=(words(i),mapper.toMap)
    iter+=temp
    
    }
    iter.toList
    }
    
    
    else List()
    
    
    
    
    }).reduceByKey((stripe1,stripe2)=>{ stripe1++stripe2 .map{ case (k,v)=> 
   k->(v+stripe1.getOrElse(k,0))
   
   }
    
    
    
    
    
    },red).map(stripe=>{
    
    var x=stripe._1

	(stripe._1,stripe._2.filter((m)=>m._2>=threshold).map{ case (k,v)=>{

	var y=broadcast_var.value(k)
	var pmi=Math.log10((total_lines.toFloat*v)/(broadcast_var.value(x).toFloat*y))
	//k +  " -> (" + pmi +","+ v + ")"
	k->(pmi,v)


}})
}).filter((m)=>m._2.size>0).map(stripe=>(stripe._1,stripe._2.toMap)).saveAsTextFile(args.output())
//  uncomment this part   map(stripe=> "(" + stripe._1+",Map("+stripe._2.mkString(" , ")+")").saveAsTextFile(args.output())
//    var y=broadcast_var.value(stripe._2._1)
  //  var pmi=Math.log10((total_lines*stripe._2._2)/(x*y))
   // (stripe._1->Map(pmi,stripe._2._1))
    








  }
}
