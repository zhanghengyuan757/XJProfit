package com.neusoft.energy.xjprofit.fileRead

import com.neusoft.energy.xjprofit.SplitEntrance.sqlc
import com.neusoft.energy.xjprofit.mytrait.TextLoadTable
@deprecated
case class r_plan (
                    mr_sect_no:String,//抄表段编号
                    mr_day:Long//抄表例日
                  )
@deprecated
object  r_plan extends TextLoadTable[r_plan]{
  override val path: String = "text/r_plan"
  override def newObject(ss: String): r_plan = {
    val str = ss.split(",")
    if(ss.endsWith(","))
      r_plan(str(1),0)
    else
      r_plan(str(1),str(2).toLong)
  }
  override def createView():Unit= {
    import sqlc.implicits._
    getRdd.toDF().registerTempTable(tableName)
  }

}