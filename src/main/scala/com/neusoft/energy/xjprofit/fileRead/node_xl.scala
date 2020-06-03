package com.neusoft.energy.xjprofit.fileRead

import com.neusoft.energy.xjprofit.SplitEntrance.sqlc
import com.neusoft.energy.xjprofit.mytrait.TextLoadTable
@deprecated
case class node_xl(
                    LINEID:String,//线路ID
                    LINENO:String,
                    LINENAME:String,
                    STARTSUBSID:String,//起点电站ID
                    STARTSUBSNAME:String,
                    ENDSUBSID:String,//终点电站ID
                    ENDSUBSNAME:String
)
@deprecated
object node_xl extends TextLoadTable[node_xl]{
  override val path: String = "text/node_xl.txt"
  override def newObject(ss: String): node_xl = {
    val str = ss.split(",")
    if(ss.endsWith(","))
      node_xl(str(1),str(0),str(1),str(2),str(2),null,null)
    else
      node_xl(str(1),str(0),str(1),str(2),str(2),str(3),str(3))
  }
  override def createView():Unit= {
    import sqlc.implicits._
    getRdd.toDF().registerTempTable(tableName)
  }
}