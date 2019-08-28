package com.Graphx

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 图计算案例
  */

object graph_test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    //构造点的集合
    //序列((值，(基本信息名字，年龄)))
    val vertexRDD = sc.makeRDD(Seq(
      (1L,("詹姆斯",35)),
      (2L,("霍华德",34)),
      (6L,("杜兰特",31)),
      (9L,("库里",30)),
      (133L,("哈登",30)),
      (138L,("席尔瓦",36)),
      (16L, ("法尔考", 35)),
      (44L, ("内马尔", 27)),
      (21L, ("J罗", 28)),
      (5L, ("高斯林", 60)),
      (7L, ("奥德斯基", 55)),
      (158L, ("码云", 55))
    ))
    //构造边的集合(三元组)
    //Edge(初始点，结束点，属性)
    val egde:RDD[Edge[Int]] = sc.makeRDD(Seq(
      Edge(1L,133L,0),
      Edge(2L,133L,0),
      Edge(6L,133L,0),
      Edge(9L,133L,0),
      Edge(6L,138L,0),
      Edge(16L, 138L, 0),
      Edge(44L, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(5L, 158L, 0),
      Edge(7L, 158L, 0)
    ))
    //构建图
    //调用构造方法Graph(点，边)
    val graph = Graph(vertexRDD,egde)
    //取出所有边上的最大顶点connectedComponents()方法
    //返回所有顶点到最大顶点（值最小的那个点）的元组
    val vertices = graph.connectedComponents().vertices
    //打印结果(158,5) (138,1) (6,1) (2,1) (16,1) (44,1) (21,1) (133,1) (1,1)....
    vertices.foreach(println)
    vertices.join(vertexRDD).map {
      //使用匹配将对应点的id换一下，conId公共点
      case (userId, (conId, (name, age))) => {
        (conId, List(name, age))
      }
    } .reduceByKey(_ ++ _)
      .foreach(println)
  }
}
