package com.neusoft.energy.xjprofit.fileRead

import com.neusoft.energy.xjprofit.mytrait.{JsonLoadTable, TextLoadTable}
import com.neusoft.energy.xjprofit.SplitEntrance.{sc, sqlc}
@deprecated
case class xl_10kv_dl(
                       lineId:String,// 线路ID
                       lineNo:String,// 线路编号
                       lineName:String,// 线路名称
                       subsId:String,// 所属变电站ID
                       subsName:String,// 电站名称
                       powerSal:BigDecimal,// 售电量
                       powerSup:BigDecimal// 供电量
                     )
@deprecated
object xl_10kv_dl extends TextLoadTable[xl_10kv_dl]{
  override val path: String = "text/xl_10kv_dl.txt"
  override def newObject(ss: String): xl_10kv_dl = {
    val str = ss.split(",")
    try {
      var powerSal=str(5)
      if(powerSal.length==0){
        powerSal="0"
      }
      var powerSup=str(6)
      if(powerSup.length==0){
        powerSup="0"
      }
      xl_10kv_dl(str(1),str(0),str(1),str(2),str(2),BigDecimal(powerSal),BigDecimal(powerSup))
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