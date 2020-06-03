package com.neusoft.energy.xjprofit.fileRead

import com.neusoft.energy.xjprofit.SplitEntrance.sqlc
import com.neusoft.energy.xjprofit.mytrait.TextLoadTable
@deprecated
case class node_station(
    subsId:String,//变电站ID
    subsNo:String,//变电站编号
    subsName:String,//变电站名称
    voltLevel:String//电压等级（划分等级要素）
)
@deprecated
object node_station extends TextLoadTable[node_station]{
  override val path: String = "text/node_station.txt"
  override def newObject(ss: String): node_station = {
    val str = ss.split(",")
    if(ss.endsWith(","))
      node_station(str(1),str(0),str(1),null)
    else
      node_station(str(1),str(0),str(1),str(2))
  }
  override def createView():Unit= {
    import sqlc.implicits._
    getRdd.toDF().registerTempTable(tableName)
  }
}
