package com.neusoft.energy.xjprofit

import java.util.Calendar

import com.neusoft.energy.xjprofit.result.{l_loss_view_cons, l_loss_view_cons_tq}
import com.neusoft.energy.xjprofit.tools.Tools
import com.neusoft.energy.xjprofit.topology.Topology
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object SplitEntrance {
  val testMode=false
  @transient lazy val log: Logger = Logger.getLogger(this.getClass)
//  val logined: Boolean =HadoopConfiguration.login("YX000003")
  val sparkConf: SparkConf = new SparkConf().setAppName("新疆利润测算项目").setMaster(if(testMode)"local[*]" else "yarn-cluster")
  val sparkSQLPath="spark_sql/spark_sql.xml"
  val propertiesPath="xjprofit.properties"
  val sc=new SparkContext(sparkConf)
  val hc=new HiveContext(sc)
  val sqlc: SQLContext =SQLContext.getOrCreate(sc)
  /**
   * @author zhanghengyuan
   * @note 分摊计算程序入口
   * @since 2019/10/11
   */
  def main(args: Array[String]): Unit = {
    val df=Tools.dateFormat
    val df_ = Tools.date_Format
      val b=df.parse("20190901")
      val e=df.parse("20191001")
      val c=Calendar.getInstance()
      c.setTime(b)
      var arr=mutable.Buffer[String]()
      while (c.getTime.before(e)){
        arr += df_.format(c.getTime)
        c.add(Calendar.DATE,1)
      }
      arr.foreach(runCal)
  }
  def runCal(ymd:String): Unit ={
    log.info(s"新疆利润测算项目($ymd):开始运行")
    val p=Tools.getWriteJBDCPropeties
    Logger.getLogger("org").setLevel(Level.WARN)
    log.info(s"新疆利润测算项目($ymd)生成用户拓扑网")
    val userTopology= Topology.getUserTopology(ymd)
    log.info(s"新疆利润测算项目($ymd)生成用户拓扑计算结果并保存")
    l_loss_view_cons.getDf(userTopology).filter("ftshdl>0").write.mode(SaveMode.Append).jdbc(p.getProperty("url"),l_loss_view_cons.tableName+"_ymd",p)
    log.info(s"新疆利润测算项目($ymd)生成台区拓扑网")
    val zoneTopology= Topology.getZoneTopology(ymd)
    log.info(s"新疆利润测算项目($ymd)生成台区拓扑计算结果并保存")
    l_loss_view_cons_tq.getDf(zoneTopology).filter("ftshdl>0").write.mode(SaveMode.Append).jdbc(p.getProperty("url"),l_loss_view_cons_tq.tableName+"_ymd",p)
    log.info(s"新疆利润测算项目($ymd)运行完毕")
  }
}
