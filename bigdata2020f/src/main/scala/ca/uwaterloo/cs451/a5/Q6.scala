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

package ca.uwaterloo.cs451.a5


import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Q6Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date= opt[String](descr = "date", required = true)
  val text = opt[Boolean](descr = "plain-text data", required = false, default=Some(false))
  val parquet = opt[Boolean](descr = "parquet data", required = false, default=Some(false))
  requireOne(text, parquet)
  verify()
}

object Q6 extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q6Conf(argv)

    log.info("Input: " + args.input())
    val conf = new SparkConf().setAppName("Q6")
    val sc = new SparkContext(conf)
    val date=args.date()
  if(args.text())
{

val our_file=args.input()+"/lineitem.tbl"
val lineitem = sc.textFile(our_file)
val customers=sc.textFile(args.input()+"/customer.tbl")
val orders=sc.textFile(args.input()+"/orders.tbl")
val nations=sc.textFile(args.input()+"/nation.tbl")

 /*select n_nationkey, n_name, count(*) from lineitem, orders, customer, nation
where
  l_orderkey = o_orderkey and
  o_custkey = c_custkey and
  c_nationkey = n_nationkey and
  l_shipdate = 'YYYY-MM-DD'
group by n_nationkey, n_name
order by n_nationkey asc;
  */
  

val line_table=lineitem.filter(p=>{
  val tokens=p.split('|')
  tokens(10).contains(date)
  }).map(p=>{
  val tokens=p.split('|')
  val l_quantity=tokens(4).toDouble
  val l_extended_price=tokens(5).toDouble
  val l_discount=tokens(6).toDouble
  val ex_dis=l_extended_price*(1.0-l_discount)
  val l_tax=tokens(7).toDouble
  val charge=ex_dis*(1.0+l_tax)
  val l_return_flag=tokens(8)
  val l_line_status=tokens(9)
  ((l_return_flag,l_line_status),(l_quantity,l_extended_price,ex_dis,charge,l_discount,1))
  }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2,x._3+y._3,x._4+y._4,x._5+y._5,x._6+y._6)).sortByKey().map(p=>(p._1._1,p._1._2,p._2._1,p._2._2,p._2._3,p._2._4,p._2._1/p._2._6,p._2._2/p._2._6,p._2._5/p._2._6,p._2._6))

line_table.collect().foreach(println)  

}
else
{
val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd

      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
      val ordersRDD = ordersDF.rdd
      
      val nationsDF = sparkSession.read.parquet(args.input() + "/nation")
      val nationsRDD = nationsDF.rdd
      
      val customersDF = sparkSession.read.parquet(args.input() + "/customer")
      val customersRDD = customersDF.rdd
      
      
  val line_table=lineitemRDD.filter(p=>{
  p(10).toString.contains(date)
  }).map(tokens=>{
  val l_quantity=tokens(4).toString.toDouble
  val l_extended_price=tokens(5).toString.toDouble
  val l_discount=tokens(6).toString.toDouble
  val ex_dis=l_extended_price*(1.0-l_discount)
  val l_tax=tokens(7).toString.toDouble
  val charge=ex_dis*(1.0+l_tax)
  val l_return_flag=tokens(8)
  val l_line_status=tokens(9)
  ((l_return_flag.toString,l_line_status.toString),(l_quantity,l_extended_price,ex_dis,charge,l_discount,1))
  }).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2,x._3+y._3,x._4+y._4,x._5+y._5,x._6+y._6)).sortByKey().map(p=>(p._1._1,p._1._2,p._2._1,p._2._2,p._2._3,p._2._4,p._2._1/p._2._6,p._2._2/p._2._6,p._2._5/p._2._6,p._2._6))

line_table.collect().foreach(println)
}
  }
}
