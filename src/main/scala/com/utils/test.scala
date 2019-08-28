package com.utils

import com.Tags.BusinessTag
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 测试类
  */
object test {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new SQLContext(sc)
   val df = ssc.read.parquet("D:\\gp22\\22dmp")
    df.map(row=>{
      val business = BusinessTag.makeTags(row)
      business
//      (v1,v2)
    }).foreach(println)
  }
}
