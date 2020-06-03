package com.neusoft.energy.xjprofit.topology

import com.neusoft.energy.xjprofit.SplitEntrance
import com.neusoft.energy.xjprofit.importResult.tq_pq
import org.apache.spark.rdd.RDD

class Zone {
  var tq_id: String = _ //台区id
  var tq_no: String = _ //台区No
  var tq_name: String = _ //台区
  var t_settle_pq: BigDecimal = _ //用户电量
  var updateTime: String = _ //更新时间
  var ssxl: String = _ //所属线路
  var costCut: Array[Cost] = _
  lazy val totalCost: BigDecimal = {
    if (costCut == null) {
      null
    } else {
      costCut.map(_.cost).sum
    }
  } //分摊损耗总额
  var wastageCut: Array[Wastage] = _
  lazy val totalWastage: BigDecimal = {
    if (wastageCut == null) {
      null
    } else {
      wastageCut.map(_.wastage).sum
    }
  } //分摊损耗总额
  def this(tq:tq_pq){
    this()
    tq_id=tq.tq_id
    tq_no=tq.tq_no
    tq_name=tq.tq_name
    t_settle_pq=tq.t_settle_pq
    updateTime=tq.date
    ssxl=tq.ssxl
  }
}
object Zone {
  def getRdd(ymd:String): RDD[Zone] = {
    tq_pq.getHiveRdd(SplitEntrance.hc).filter(_.date==ymd).map(new Zone(_))
  }
}
