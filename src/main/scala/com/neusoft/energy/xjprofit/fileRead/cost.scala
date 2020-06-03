package com.neusoft.energy.xjprofit.fileRead

import com.neusoft.energy.xjprofit.SplitEntrance.sqlc
import com.neusoft.energy.xjprofit.mytrait.TextLoadTable
@deprecated
case class cost(
        id:String,//流水ID
        sbid:String,//设备ID
        sbmc:String,//设备名称
        sbtype:String,//设备类型
        cost:BigDecimal,//成本金额
        costName:String,//成本名称
        ymd:String//更新时间
)
@deprecated
object cost extends TextLoadTable[cost] {

  override val path: String = "text/cost"
  override def newObject(ss: String): cost = {
    val str = ss.split("\t")
    cost(str(0),str(1),str(2),str(3),BigDecimal(str(4)),str(5),str(6))
  }
  override def createView():Unit= {
    import sqlc.implicits._
    getRdd.toDF().registerTempTable(tableName)
  }
}
