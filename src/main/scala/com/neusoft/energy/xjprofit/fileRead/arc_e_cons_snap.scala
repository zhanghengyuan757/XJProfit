package com.neusoft.energy.xjprofit.fileRead

import com.neusoft.energy.xjprofit.SplitEntrance.sqlc
import com.neusoft.energy.xjprofit.mytrait.TextLoadTable
@deprecated
case class arc_e_cons_snap (
                             cons_id:Long,//用户id
                             org_no:String,//组织编号
                             mr_sect_no:String//抄表例日
                            )
@deprecated
object  arc_e_cons_snap extends TextLoadTable[arc_e_cons_snap]{
  override val path: String = "text/arc_e_cons_snap"
  override def newObject(ss: String): arc_e_cons_snap = {
    val str = ss.split(" ")
    arc_e_cons_snap(str(1).toLong,str(2),str(3))
  }
  override def createView():Unit= {
    import sqlc.implicits._
    getRdd.toDF().registerTempTable(tableName)
  }

}