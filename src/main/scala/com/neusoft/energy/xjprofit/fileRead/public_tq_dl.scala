package com.neusoft.energy.xjprofit.fileRead

import com.neusoft.energy.xjprofit.SplitEntrance.sqlc
import com.neusoft.energy.xjprofit.mytrait.TextLoadTable
@deprecated
case class public_tq_dl(
                         tgId:String, //台区ID
                         tgNo:String, //台区编号
                         tgName:String, //台区名称
                         lineId:String,//所属线路ID
                         lineName:String, //所属线路
                         powerSal:BigDecimal, //售电量
                         powerSup:BigDecimal//供电量
     )
@deprecated
object public_tq_dl extends TextLoadTable[public_tq_dl]{
  override val path: String = "text/public_tq_dl.txt"
  override def newObject(ss: String): public_tq_dl = {
    val str = ss.split(",")
    try {
    if(ss.endsWith(","))
      public_tq_dl(str(1),str(0),str(1),str(2),str(2),BigDecimal(str(5)),null)
    else
      public_tq_dl(str(1),str(0),str(1),str(2),str(2),BigDecimal(str(5)),BigDecimal(str(6)))
    } catch {
      case e:Exception =>
        println("无法处理的数据："+str.mkString(",")+"，原数据："+ss)
        null
    }
  }
  override def createView():Unit= {
    import sqlc.implicits._
    getRdd.toDF().registerTempTable(tableName)
  }
}