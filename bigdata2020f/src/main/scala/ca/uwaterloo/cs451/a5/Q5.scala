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

class Q5Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val text = opt[Boolean](descr = "plain-text data", required = false, default=Some(false))
  val parquet = opt[Boolean](descr = "parquet data", required = false, default=Some(false))
  requireOne(text, parquet)
  verify()
}

object Q5 extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q5Conf(argv)

    log.info("Input: " + args.input())
    val conf = new SparkConf().setAppName("Q5")
    val sc = new SparkContext(conf)
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
  
val customer_table=customers.map(p=>{
val tokens=p.split('|')
(tokens(0),tokens(3).toInt)
}).collectAsMap()
val customer_hashed=sc.broadcast(customer_table)
  
val nation_table=nations.map(p=>{
val tokens=p.split('|')
(tokens(1),tokens(0).toInt)
}).collectAsMap()    //emit nation_name and nation_key pair
val nation_hashed=sc.broadcast(nation_table)
  
  val us_key=nation_hashed.value("UNITED STATES")
  val ca_key=nation_hashed.value("CANADA")
  
  val orders_table=orders.map(p=>{
val tokens=p.split('|')
(tokens(0),tokens(1))
}).filter(p=>{
  val cust_key=p._2
  val nation_key=customer_hashed.value(cust_key)
  nation_key==us_key || nation_key==ca_key
  })
  
  
  val line_item_orders=lineitem
  .map(p=>{
  val tokens=p.split('|')
  (tokens(0),tokens(10))   // l_order_key,shipdate
  })
  //combine lineitem and orders_table

val nation_table_get_name=nations.map(p=>{
val tokens=p.split('|')
(tokens(0).toInt,tokens(1))
}).collectAsMap()    //emit nation_name and nation_key pair
val nation_hashed_get_name=sc.broadcast(nation_table_get_name)
  
  val ans=line_item_orders.cogroup(orders_table).filter(p=>{
  p._2._2.iterator.hasNext
  }).flatMap(p=>{
  val cust_key=p._2._2.iterator.next()
  //map shipdate-(cust_key,number_of_shipments)
  p._2._1.map(shipdate=>((customer_hashed.value(cust_key),shipdate.slice(0,7)),1))
  }).reduceByKey(_ + _).sortByKey().collect().map(p=>(p._1._1,nation_hashed_get_name.value(p._1._1),p._1._2,p._2)).foreach(println)
  
  
   
  
  
/*
  
val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
      val orders = sc.textFile(args.input() + "/orders.tbl")
      val customer = sc.textFile(args.input() + "/customer.tbl")
      val nation = sc.textFile(args.input() + "/nation.tbl")

      val nationkeys = nation
        .map(line => {
          val tokens = line.split('|')
          (tokens(1), tokens(0).toInt)
        })
        .collectAsMap()
      val nationkeysHMap = sc.broadcast(nationkeys)
      val nationkey_ca = nationkeysHMap.value("CANADA")
      val nationkey_us = nationkeysHMap.value("UNITED STATES")

      val custkeys = customer
        .map(line => {
          val tokens = line.split('|')
          (tokens(0), tokens(3).toInt)
        })
        .collectAsMap()
      val custkeysHMap = sc.broadcast(custkeys)

      val l_orderkeys = lineitem
        .map(line => {
          val tokens = line.split('|')
          (tokens(0), tokens(10))
        })

      val o_orderkeys = orders
        .map(line => {
          val tokens = line.split('|')
          (tokens(0), tokens(1))
        })
        .filter(p => {
          val nationkey = custkeysHMap.value(p._2)
          nationkey == nationkey_ca || nationkey == nationkey_us
        })

      l_orderkeys.cogroup(o_orderkeys)
        .filter(p => p._2._2.iterator.hasNext)
        .flatMap(p => {
          val custkey = p._2._2.iterator.next()
          p._2._1.map(shipdate => ((custkeysHMap.value(custkey), shipdate.slice(0,7)), 1))
        })
        .reduceByKey(_ + _)
        .sortByKey()
        .collect()
     //   .foreach(p => println(p._2))
        .foreach(p => println("(" + p._1._1 + "," + p._1._2 + "," + p._2 + ")"))

*/

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
      

val customer_table=customersRDD.map(p=>{
(p(0),p(3).toString.toInt)
}).collectAsMap()
val customer_hashed=sc.broadcast(customer_table)

val nation_table=nationsRDD.map(p=>{
(p(1),p(0).toString.toInt)
}).collectAsMap()    //emit nation_name and nation_key pair
val nation_hashed=sc.broadcast(nation_table)

  val us_key=nation_hashed.value("UNITED STATES")
  val ca_key=nation_hashed.value("CANADA")

  val orders_table=ordersRDD.map(p=>{
(p(0),p(1))
}).filter(p=>{
  val cust_key=p._2
  val nation_key=customer_hashed.value(cust_key)
  nation_key==us_key || nation_key==ca_key
  })


  val line_item_orders=lineitemRDD
  .map(p=>{
  (p(0),p(10))   // l_order_key,shipdate
  })
  //combine lineitem and orders_table

val nation_table_get_name=nationsRDD.map(line=>{
(line(0).toString.toInt,line(1))
}).collectAsMap()    //emit nation_name and nation_key pair
val nation_hashed_get_name=sc.broadcast(nation_table_get_name)

  val ans=line_item_orders.cogroup(orders_table).filter(p=>{
  p._2._2.iterator.hasNext
  }).flatMap(p=>{
  val cust_key=p._2._2.iterator.next()
  //map shipdate-(cust_key,number_of_shipments)
  p._2._1.map(shipdate=>((customer_hashed.value(cust_key),shipdate.toString.substring(0,7)),1))
  }).reduceByKey(_ + _).sortByKey().collect().map(p=>(p._1._1,nation_hashed_get_name.value(p._1._1),p._1._2,p._2)).foreach(println)
}
  }
}
