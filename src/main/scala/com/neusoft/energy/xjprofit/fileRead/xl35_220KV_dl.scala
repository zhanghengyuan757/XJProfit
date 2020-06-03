package com.neusoft.energy.xjprofit.fileRead

import com.neusoft.energy.xjprofit.SplitEntrance.sqlc
import com.neusoft.energy.xjprofit.mytrait.TextLoadTable
@deprecated
case class xl35_220KV_dl(
                        lineId:String , //线路ID
                        lineName:String,
                        lineNo:String,
                        startSubsId:String , //起始变电站ID
                        startSubsName:String,
                        endSubsId:String , //终点电站ID
                        endSubsName:String,
                        powerin:BigDecimal,
                        powerout:BigDecimal
)
@deprecated
object xl35_220KV_dl extends TextLoadTable[xl35_220KV_dl]{
  override val path: String = "text/xl35_220KV_dl.txt"
  override def newObject(ss: String): xl35_220KV_dl = {
    val str = ss.split(",")
    if(ss.endsWith(","))
      xl35_220KV_dl(str(0),str(0),str(1),str(2),str(2),str(3),str(3),BigDecimal(str(4)),null)
    else
      xl35_220KV_dl(str(0),str(0),str(1),str(2),str(2),str(3),str(3),BigDecimal(str(4)),BigDecimal(str(5)))
  }
  override def createView():Unit= {
    import sqlc.implicits._
    getRdd.toDF().registerTempTable(tableName)
  }
}