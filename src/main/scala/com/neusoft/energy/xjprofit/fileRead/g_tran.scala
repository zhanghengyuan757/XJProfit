package com.neusoft.energy.xjprofit.fileRead

import com.neusoft.energy.xjprofit.SplitEntrance.sqlc
import com.neusoft.energy.xjprofit.mytrait.TextLoadTable
@deprecated
case class g_tran(
                   tg_id: Long, //用户id
                   cons_id: Long, //用户id
                   equip_id: String, //变压器id（专变）
                   pms_equip_id: String //变压器id（公变）
                 )
@deprecated
object g_tran extends TextLoadTable[g_tran] {
  override val path: String = "text/g_tran"

  override def newObject(ss: String): g_tran = {
    val str = ss.split("[,]")
    if(ss.endsWith(","))
      g_tran(str(1).toLong, str(2).toLong, str(3), null)
    else
      g_tran(str(1).toLong, str(2).toLong, str(3), str(4))
  }

  override def createView(): Unit = {
    import sqlc.implicits._
    getRdd.toDF().registerTempTable(tableName)
  }
}