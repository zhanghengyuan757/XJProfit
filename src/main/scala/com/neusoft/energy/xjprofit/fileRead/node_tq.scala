package com.neusoft.energy.xjprofit.fileRead

import com.neusoft.energy.xjprofit.SplitEntrance.sqlc
import com.neusoft.energy.xjprofit.mytrait.TextLoadTable
@deprecated
case  class node_tq(
          tgId:String,//台区ID
          tgNo:String,
          tgName:String,
          lineId:String,//所属线路ID
          lineName:String
)
@deprecated
object node_tq extends TextLoadTable[node_tq]{
  override val path: String = "text/node_tq.txt"
  override def newObject(ss: String): node_tq = {
    val str = ss.split(",")
    if(ss.endsWith(","))
      node_tq(str(1),str(0),str(1),null,null)
    else
      node_tq(str(1),str(0),str(1),str(2),str(2))
  }
  override def createView():Unit= {
    import sqlc.implicits._
    getRdd.toDF().registerTempTable(tableName)
  }
}



