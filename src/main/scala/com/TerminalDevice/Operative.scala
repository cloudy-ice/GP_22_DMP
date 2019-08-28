package com.TerminalDevice

import com.utils.RptUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object Operative {
  def main(args: Array[String]): Unit = {
    def main(args: Array[String]): Unit = {
      //判断路径是否正确
      if(args != 1){
        println("目录参数不正确")
        sys.exit(0)
      }
      //创建一个集合保存目录
      val Array(inputPath) = args
      //初始化
      val conf = new SparkConf().setAppName("this.getClass.getName").setMaster("local[*]")
        .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      val sc = new SparkContext(conf)
      val sQLContext = new SQLContext(sc)

      val df = sQLContext.read.parquet(inputPath)

      val mapped  = df.map(row=> {
        // 把需要的字段全部取到
        val requestmode = row.getAs[Int]("requestmode")
        val processnode = row.getAs[Int]("processnode")
        val iseffective = row.getAs[Int]("iseffective")
        val isbilling = row.getAs[Int]("isbilling")
        val isbid = row.getAs[Int]("isbid")
        val iswin = row.getAs[Int]("iswin")
        val adorderid = row.getAs[Int]("adorderid")
        val WinPrice = row.getAs[Double]("winprice")
        val adpayment = row.getAs[Double]("adpayment")
        //  判断appname,appname做key
        val ispname = row.getAs[String]("ispname")

        val list1 = RptUtils.request(requestmode, processnode)
        val list2 = RptUtils.click(requestmode, iseffective)
        val list3 = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)

        (ispname,(list1 ++ list2 ++ list3))
      })
        .reduceByKey((list1,list2) => {
          //List((1,1),(2,2))
          list1.zip(list2).map(t => t._1 + t._2)
        })
    }
  }

}
