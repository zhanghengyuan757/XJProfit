package com.neusoft.energy.xjprofit.importResult

import com.neusoft.energy.xjprofit.SplitEntrance.sqlc
import com.neusoft.energy.xjprofit.fileRead._
import com.neusoft.energy.xjprofit.mytrait.{DataImportResult, HiveTable}
import com.neusoft.energy.xjprofit.tools.Tools
import org.apache.spark.rdd.RDD
case class equipment_relationship_pq( //拓扑电量关系表
    sbid: String, //设备ID
    sbbm: String, //设备编码
    sbmc: String, //设备名称
    psbid: String, //上级设备ID
    sbtype: String, //设备类型
    voltlevel: String, //电压等级
    in_settle_pq: BigDecimal, //设备入电量
    out_settle_pq: BigDecimal, //设备出电量
    updatetime: String //更改时间
)
object equipment_relationship_pq extends
   DataImportResult[equipment_relationship_pq]
  with HiveTable[equipment_relationship_pq] {
  override def runSQL: RDD[equipment_relationship_pq] = {
    val tables=Set(node_tq,node_xl,public_tq_dl,node_station,xl_10kv_dl,xl35_220KV_dl)
    tables.foreach(_.createView())
    val result: RDD[equipment_relationship_pq] =sqlc.sql(Tools.getSql(tableName)).map(newObjectFromRow[equipment_relationship_pq](_))
//    println("equipment_relationship_pq Size:"+result.count())
    result
  }
}


