package com.neusoft.energy.xjprofit.mytrait

import com.neusoft.energy.xjprofit.tools.Tools
import org.apache.spark.rdd.RDD
@deprecated
trait TextLoadTable[T] extends Reflective {
  val path:String
  def newObject(str: String):T
  def getRdd(implicit mf: scala.reflect.Manifest[T]): RDD[T]= {
    val result=Tools.getTransFromText(path).filter(!_.endsWith(",,")).map(m=>newObject(m))
    result.filter(_!=null)
  }
  def createView()
}
