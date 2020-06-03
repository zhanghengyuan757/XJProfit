package com.neusoft.energy.xjprofit.topology

import com.neusoft.energy.xjprofit.SplitEntrance
import com.neusoft.energy.xjprofit.importResult.{equipment_cost, equipment_relationship_pq}
import com.neusoft.energy.xjprofit.tools.Tools
import org.apache.spark.rdd.RDD

/**
 * @author zhanghengyuan
 * @note 拓扑网络节点 设备类型：电站 线路 柱变 配电 低压线路 接入点 计量箱 台区
 * @since 2019/9/23
 */
class Node extends Serializable {
  var id: BigInt = _ //流水id
  var sbid: String = _ //设备id或台区ID
  var psbid: String = _ //上级设备id
  var psbidList: Array[String] = _ //上级设备id List
  var sbbm: String = _ //设备编码
  var sbmc: String = _ //设备名称
  var sbtype: String = _ //设备类型
  var voltLevel: String = _ // TODO 电压等级
  var in_settle_pq: BigDecimal = _ //设备入电量
  var out_settle_pq: BigDecimal = _ //设备出电量
  var updatetime: String = _ //更改时间
  var wastage: Wastage = _ //损耗
  var costs: Array[Cost] = _ //成本
  var splitPercent: BigDecimal = _

  /**
   * @author zhanghengyuan
   * @note 在拓扑内找孩子节点
   * @since 2019/10/24
   */
  def getchildInTopo(topo: Array[Node]): Array[Node] = {
    topo.filter(_.psbidList.contains(sbid))
  }

  def this(e: equipment_relationship_pq) {
    this()
    sbid = e.sbid
    psbid = e.psbid
    sbbm = e.sbbm
    sbmc = e.sbmc
    voltLevel = e.voltlevel
    sbtype = e.sbtype
    in_settle_pq = e.in_settle_pq
    out_settle_pq = e.out_settle_pq
    updatetime = e.updatetime
  }
}

object Node {
  def getRdd(ymd: String): RDD[Node] = {
    val hc = SplitEntrance.hc
    val rdd = equipment_relationship_pq.getHiveRdd(hc).filter(_.updatetime == ymd).map(new Node(_))
    val ym = Tools.ymd_2Ym(ymd)
    val costRdd = equipment_cost.getHiveRdd(hc)
      .filter(_.ym == ym).map(new Cost(_)).groupBy(_.sbid)
    val groupedRdd = rdd.groupBy(_.sbid).map(kv => {
      val node = kv._2.head
      val psbid = kv._2.map(_.psbid)
      node.psbidList = psbid.toArray
      node.wastage = new Wastage(node)
      node
    })
    val brotherTotalPqRdd = groupedRdd.groupBy(_.psbidList.mkString(",")).map(i => {
      val pSBIDs = i._1.split(",")
      if (pSBIDs.size > 1) {
        (i._1, null) //有多个父亲节点的为变电站,没有兄弟节点
      } else {
        (i._1, i._2.map(_.in_settle_pq).sum) //只有一个父亲节点的一般为用户或线路或只有一个线路的变电站
      }
    })
    val wastageSetedRdd = groupedRdd.groupBy(_.psbidList.mkString(",")).join(brotherTotalPqRdd).map(i => {
      val result = i._2._1
      val brotherTotalPq = i._2._2
      result.map(r => {
        if (brotherTotalPq == null) {
          r.splitPercent = 1
        } else {
          if (brotherTotalPq == 0) {
            r.splitPercent = 0
          } else {
            r.splitPercent = r.in_settle_pq / brotherTotalPq
          }
        }
        r
      })
    }).flatMap(x => x)
    wastageSetedRdd.map(n => (n.sbbm, n))// 使用设备编码关联
      .leftOuterJoin(costRdd).map(i => {
      val cost = i._2._2.orNull
      val node = i._2._1
      node.costs = if (cost != null) {
        cost.toArray
      } else {
        null
      }
      node
    })
  }


}