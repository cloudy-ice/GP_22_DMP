package com.Test1

import com.utils.JDBCConnectePoolsTest
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object VIP1 {
  def main(args: Array[String]): Unit = {
    //初始化
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    //创建装点的容器
//    val buffer = collection.mutable.ListBuffer[String]()
//    var list =List[(Long,(String,Int))]()
    //创建RDD
    val data: RDD[String] = sc.makeRDD(List("1	小红 20", "3 小明 33", "5	小七	20", "7	小王 60", "9 小李	20", "11	小美	30", "13 小花"))
    //构造点的集合
    val vertexRDD: RDD[(Long, (String, Int))] = data.map(line => {
      val arr = line.split("\t|\\s")
      if (arr.size > 2) {
        (arr(0).toLong, (arr(1), arr(2).toInt))
      } else null
    }).filter(_ != null) //   .foreach(println)

    //构造边的集合
    val edge = sc.makeRDD(Seq(
      Edge(1L,3L,0),
      Edge(11L,9L,0),
      Edge(9L,3L,0),
      Edge(5L,7L,0),
      Edge(13L,3L,0)
    ))

    //使用图的构造方法构建图
    val graph = Graph(vertexRDD,edge)
    val vertices = graph.connectedComponents().vertices
    //(11,1)
    //(3,1)
    //(7,5)
    val res = vertices.join(vertexRDD).map({
      case (uid,(conid,(uname,uage))) => {
        (conid,List(uname,uage))
      }
    }).reduceByKey(_ ++ _)
      .foreach(println)

    //存入数据库

//    res.foreachPartition(tup => {
//      val conn = JDBCConnectePoolsTest.getConn()
//      tup.foreach(one =>{
//        val pstm = conn.prepareStatement("insert into test(uid,relation) values(?,?)")
//        pstm.setLong(1,one._1)
//        pstm.setString(2,one._2.toString())
//        pstm.executeUpdate()
//      })
//      //还连接
//      JDBCConnectePoolsTest.returnConn(conn)
//    })

    sc.stop()
  }
}
