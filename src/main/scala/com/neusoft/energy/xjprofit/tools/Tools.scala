package com.neusoft.energy.xjprofit.tools

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import com.neusoft.energy.xjprofit.SplitEntrance._
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.xml.XML

object Tools {
  val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
  val date_Format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  val ymFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMM")
  private val ymMap=mutable.Map[String,Int]()
  def getCurrentYmd: String = {
    val now: Date = new Date()
    val date = dateFormat.format(now)
    date
  }
  def ymd_2Ym(y_m_d:String):String={
    val arr=y_m_d.split("[-]")
    if(arr.length==3){
      arr(0)+arr(1)
    }else{
      null
    }
  }
  def getMonthDaysByYm(ym:String):Int={
    if(!ymMap.contains(ym)){
      val c=Calendar.getInstance()
      c.setTime(ymFormat.parse(ym))
      val i=c.getActualMaximum(Calendar.DAY_OF_MONTH)
      ymMap(ym)=i
    }
    ymMap(ym)
  }

  def getTransFromText(path: String): RDD[String] = {
    sc.textFile(path)
  }

  def getJsonObject(path: String): List[Map[String, Object]] = {
    import scala.io.Source
    import scala.util.parsing.json.JSON
    val p = this.getClass.getClassLoader.getResource(path).getPath
    val source =Source.fromFile(p, "GBK")
    val str = source.mkString.replaceAll("\\s+|\t+|\r|\n", "")
    val result = JSON.parseFull(str)
    source.close()
    val resultValue = result.get.asInstanceOf[Map[String, Object]]("resultValue")
    val items = resultValue.asInstanceOf[Map[String, Object]]("items")
    items.asInstanceOf[List[Map[String, Object]]]
  }

  /**
   * @note 配置文件参数获取器
   * @since 2019/6/12
   * @author zhanghy
   */
  def getProperties(k: String): String = {
    val p = new Properties()
    p.load(getClass.getClassLoader.getResourceAsStream(propertiesPath))
    p.getProperty(k)
  }
  lazy val getWriteJBDCPropeties:Properties={
    val p = new Properties()
    p.setProperty("user",getProperties("jdbc.user"))
    p.setProperty("driver",getProperties("jdbc.driver"))
    p.setProperty("url",getProperties("jdbc.url"))
    p.setProperty("password",getProperties("jdbc.password"))
    p
  }

  /**
   * @note PgSql解析器
   * @since 2019/6/12
   * @author zhanghy
   */
  def getSql(id: String): String = {
    val sqlSource = XML.load(getClass.getClassLoader.getResourceAsStream(sparkSQLPath))
    val n = (sqlSource \\ "sql").filter(_.attribute("id").exists(_.text.equals(id))).head
    n.text.trim().replaceAll("\\s+|\t+|\r|\n", " ")
  }

  /**
   * @author zhanghengyuan
   * @note 获取所有电压等级
   * @since 2019/10/22
   */
  def getVoltLevel: List[(Int,String)] = {
    import scala.io.Source
    import scala.util.parsing.json.JSON
    val stream = this.getClass.getClassLoader.getResourceAsStream("json/voltLevel.json")
    val source =Source.fromInputStream(stream)
    val str = source.mkString.replaceAll("\\s+|\t+|\r|\n", "")
    val result = JSON.parseFull(str)
    source.close()
    val resultValue = result.get.asInstanceOf[Map[String, Object]]("items")
    resultValue.asInstanceOf[List[Map[String, String]]].map(m=>(m("value").toInt,m("text")))
  }
  lazy val voltLevelMap: Map[String, Int] =getVoltLevel.map(i=>(i._2,i._1)).toMap
}
