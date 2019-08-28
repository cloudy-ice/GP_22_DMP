package com.Rpt

import org.apache.hadoop.io.retry.RetryUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import com.utils.RptUtils
import org.apache.spark.rdd.RDD

/**
  * 地域分布指标
  */
object LocationRpt {
  def main(args: Array[String]): Unit = {
    //设置hadoop的驱动程序，即环境变量
//    System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin-master")
    // 判断路径是否正确
    if(args.length != 1) {
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    val Array(inputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 获取数据
    val df: DataFrame = sQLContext.read.parquet(inputPath)

    // 将数据进行处理，统计各个指标
    val mapped  = df.map(row=>{
      // 把需要的字段全部取到
      //数据请求方式（1:请求、2:展示、3:点击）
      val requestmode = row.getAs[Int]("requestmode")
      //流程节点（1：请求量 kpi 2：有效请求 3：广告请求）
      val processnode = row.getAs[Int]("processnode")
      //有效标识（有效指可以正常计费的）(0：无效 1：有效
      val iseffective = row.getAs[Int]("iseffective")
      //是否收费（0：未收费 1：已收费）
      val isbilling = row.getAs[Int]("isbilling")
      //是否 rtb
      val isbid = row.getAs[Int]("isbid")
      //是否竞价成功
      val iswin = row.getAs[Int]("iswin")
      //广告 id
      val adorderid = row.getAs[Int]("adorderid")
      //rtb 竞价成功价格
      val WinPrice = row.getAs[Double]("winprice")
      //转换后的广告消费
      val adpayment = row.getAs[Double]("adpayment")
      // key 值  是地域的省市
      val pro = row.getAs[String]("provincename")
      val city = row.getAs[String]("cityname")
      // 创建三个对应的方法处理九个指标

      val list1 = RptUtils.request(requestmode,processnode)
      val list2 = RptUtils.click(requestmode,iseffective)
      val list3 = RptUtils.Ad(iseffective,isbilling,isbid,iswin,adorderid,WinPrice,adpayment)
      val res = list1 ::: list2 ::: list3

      //原始请求数，有效请求数，广告请求数，展示数，点击数，参与竞价数，竞价成功数，DSP广告消费，DSP广告成本
      ((pro,city),res)
    })
    //根据Key聚合value
       .reduceByKey((list1,list2) => {
      //List((1,1),(2,2))
      list1.zip(list2).map(t => t._1 + t._2)
    })

      .map(t => {
      t._1 + "," + t._2.mkString(",")
    })

    mapped.collect().foreach(println)
    //如果存入mysql的话，需要使用foreachPartition
    //自己写一个连接池

  }
}
