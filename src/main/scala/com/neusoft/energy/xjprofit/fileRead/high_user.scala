package com.neusoft.energy.xjprofit.fileRead

import com.neusoft.energy.xjprofit.SplitEntrance.sqlc
import com.neusoft.energy.xjprofit.mytrait.TextLoadTable
@deprecated
case class high_user(
    consNo:String,//用户No
    consName:String,//用户名称
    lineId:String,//所属线路Id
    lineName:String//所属线路名称
)
@deprecated
object high_user extends TextLoadTable[high_user]{
  override val path: String = "text/high_user.txt"
  override def newObject(ss: String): high_user = {
    val str = ss.split(",")
    if(ss.endsWith(","))
      high_user(str(0),str(1),null,null)
    else
      high_user(str(0),str(1),str(2),str(2))
  }
  override def createView():Unit= {
    import sqlc.implicits._
    getRdd.toDF().registerTempTable(tableName)
  }
}