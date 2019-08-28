package com.Tags

import com.utils.{JedisConnectionPool, TagUtils}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 上下文标签
  */
object TagsContext2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
    if(args.length != 4){
      println("目录不匹配，退出程序")
      sys.exit()
    }
    val Array(inputPath,outputPath,dirPath,stopPath)=args
    // 创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 读取数据
    val df = sQLContext.read.parquet(inputPath)
    // 获取停用词库
    val stopword = sc.textFile(stopPath).map((_,0)).collectAsMap()
    val bcstopword = sc.broadcast(stopword)
    // 过滤符合Id的数据
    df.filter(TagUtils.OneUserId)
      // 接下来所有的标签都在内部实现
        .mapPartitions(row=>{
      val jedis = JedisConnectionPool.getConnection()
      var list = List[(String,List[(String,Int)])]()
      row.map(row=>{
        // 取出用户Id
        val userId = TagUtils.getOneUserId(row)
        // 接下来通过row数据 打上 所有标签（按照需求）
        val adList = TagsAd.makeTags(row)
        val appList = TagAPP.makeTags(row,jedis)
        val keywordList = TagKeyWord.makeTags(row,bcstopword)
        val dvList = TagDevice.makeTags(row)
        val loactionList = TagLocation.makeTags(row)
        list:+=(userId,adList++appList++keywordList++dvList++loactionList)
      })
      jedis.close()
      list.iterator
        })
      .reduceByKey((list1,list2)=>
        // List(("lN插屏",1),("LN全屏",1),("ZC沈阳",1),("ZP河北",1)....)
        (list1:::list2)
          // List(("APP爱奇艺",List()))
          .groupBy(_._1)
          .mapValues(_.foldLeft[Int](0)(_+_._2))
          .toList
      ).foreach(println)

  }
}
