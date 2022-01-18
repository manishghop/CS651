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


class PairsPMIConf (args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold", required = false, default = Some(10))
  val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
  val executorCores = opt[Int](descr = "number of cores", required = false, default = Some(1))
  verify()
}


object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new PairsPMIConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("PairsPMI")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val interDir=new Path("cs451-inter-pairspmi")
    FileSystem.get(sc.hadoopConfiguration).delete(interDir,true)
    val path="cs451-inter-pairspmi"
    val threshold=args.threshold()
    val red=args.reducers()
    var te=sc.textFile(args.input())
    var total_lines=te.count()
    val words=te.flatMap(line=>{
    var tokens=tokenize(line)
    if(tokens.length>1)
    {
    tokens.take(Math.min(40,tokens.length)).distinct

    }
    else List()
    }).map(word=>(word,1.0)).reduceByKey(_+_,red).collectAsMap()

    var broad_res=sc.broadcast(words)


//    var txt=sc.textFile(args.input())
    var total_count=0
    te.flatMap(line=>{
    var tokens=tokenize(line)
	val words = tokens.take(Math.min(tokens.length, 40)).distinct
    if(words.length>1)
    {
    var pairs=scala.collection.mutable.ListBuffer[(String,String)]()
    var len=words.length
    var i=0
    var j=0
    for(i<- 0 to len-1)
    {
    for(j<- 0 to len-1)
    {
    if((i!=j) && (words(i)!=words(j)))
    {
    var pair: (String,String) =(words(i),words(j))
    pairs+=pair
    }
    }
    }
    pairs.toList
    }

    //total_count+=1

    else List()
    // we can have a hashmap of word count

    }).map(p=>(p,1.0)).reduceByKey(_+_,red).filter((m)=>m._2>=threshold).map(p=>{
    var x_count=broad_res.value(p._1._1)
    var y_count=broad_res.value(p._1._2)
    var v=p._2
    var pmi=Math.log10((total_lines*v)/(y_count*x_count))
    (p._1,(pmi,v.toInt))
    }).saveAsTextFile(args.output())









/*    val te=sc.textFile(args.input())
    te.flatMap(line=>{
    var tokens=tokenize(line)
//    tokens=tokenize(tokens)
   //println(tokens)
    if(tokens.length>1)
    {
    tokens.map(p=>(p,1.0))

    }
    else List()
    }).reduceByKey(_+_).collectAsMap.saveAsTextFile(path)

val temp=sc.textFile(path)
var mapper=scala.collection.mutable.Map[String,Float]()
   // val temp=sc.textFile("cs451-inter")
    for(x <-temp)
    {   println(x)
        var line=x.replaceAll("\\(","").replaceAll("\\)","")
        println(line)
    var y=line.split(",")
//      print(y)
        println(y.length)
        if(y.length==2)
        {
        println(y(0))
        println(y(1))
    mapper(y(0))=y(1).toFloat
        println(mapper(y(0)))
        }
    }
 mapper.keys.foreach{i=>
        print("Key = "+i)
        println("Value = "+mapper(i))
        }

println("FDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDddd")
println(mapper.get("comment"))
println(mapper.size)
    val txt=sc.textFile(args.input())
    var total_count=0
    txt.flatMap(line=>{
    val tokens=tokenize(line)
    total_count+=1
    if(tokens.length>1)
    {
        tokens.sliding(2).map(p=>(p.head,p.tail.mkString(" ")))
    }
    else List()


    }).map(p=>(p,1.0)).reduceByKey(_+_).saveAsTextFile(args.output())
//    val z=te++txt
 //.map{case (k,v)=> {
    //val pmi=total_count*v/part2.get(_._1)*part1.get(_._1)

    //(k,(pmi,v))
    //}}

//    txt.saveAsTextFile(args.output())














  val textFile = sc.textFile(args.input(), args.reducers())
    textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) {
          tokens.sliding(1).map(p =>{(p.head,1.0)
        })
          // val bigramMarginal = tokens.init.map(w => (w, "*")).toList
          // bigram ++ bigramMarginal
        } else List()

        }).reduceByKey(_+_).saveAsTextFile("cs451-inter-pairspmi")

val text =sc.textFile(args.input(),args.reducers())
text.flatMap(line=>  {
        val tokens=tokenize(line)
        if(tokens.length>1){
        tokens.sliding(2).map(p=> (p.head,p.tail.mkString("")))
        }
else List()
}).map(p=>(p,1)).reduceByKey(_+_).saveAsTextFile(args.output())



This was earlier comment not working  down waala

      }).reduceByKey((a,b)=>{a++b.map{case (k,v) => k-> (v+a.getOrElse(k,0.0))}}).map(stripe=>{ val sum=stripe._2.foldLeft(0.0)(_+_._2)
 (stripe._1,stripe._2 map {case (k,v)=> k+"=" +(v/sum)      } ) }).map(s=> s._1 +" { "+ s._2.mkString(" ,")+"}" )
.saveAsTextFile(args.output())
//val t=textFile.reduceByKey((a,b)=>{a++b}.map((k,v)=>{ case (k,v) k->                  )
//t.saveAsTextFile("data-check")

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
