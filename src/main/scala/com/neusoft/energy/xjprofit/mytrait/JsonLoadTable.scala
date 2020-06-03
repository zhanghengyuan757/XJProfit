package com.neusoft.energy.xjprofit.mytrait

import com.neusoft.energy.xjprofit.tools.Tools
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
@deprecated
trait JsonLoadTable[T] extends Reflective {
  override val tableName=this.getClass.getSimpleName.replaceAll("[$]","")
  val path: String

  def getObjectList(implicit mf: scala.reflect.Manifest[T]): List[T] = {
    Tools.getJsonObject(path).map(m => newObjectFromMap[T](m))
  }

  def getRdd(sc:SparkContext)(implicit mf: scala.reflect.Manifest[T]): RDD[T] = {
    sc.parallelize(getObjectList)
  }

  def createView()
}
