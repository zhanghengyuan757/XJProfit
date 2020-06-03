package com.neusoft.energy.xjprofit.topology

import com.neusoft.energy.xjprofit.SplitEntrance
import com.neusoft.energy.xjprofit.importResult.{tq_cons_pq, user_info}
import org.apache.spark.rdd.RDD

/**
 * @author zhanghengyuan
 * @note 用户
 * @since 2019/9/23
 */

class User extends Serializable {
  var cons_id: BigInt = _ //用户ID
  var cons_name: String = _ //用户ID
  var cons_no: String = _ //用户编号
  var tq_id: String = _ //台区id
  var tq_name:String =_//台区名称
  var pub_priv_flag: String = _ //用电类别(01公变02专变)
  var mr_day: Long = _ //抄表例日
  var ssds: String = _ //所属地市编码
  var ssdsmc: String = _ //所属地市名称
  var ywdwmc: String = _ //运维单位名称
  var sgsbh: String = _ //省公司编号（预留）
  var t_settle_pq: BigDecimal = _ //用户电量
  var updateTime: String = _ //更新时间
  var costCut: Array[Cost] = _ //摊到台区或变压器或专变用户身上的成本
  lazy val totalCost: BigDecimal = {
    if (costCut == null) {
      null
    } else {
      if (brotherTotalPq == 0)
        0
      else
      costCut.map(_.cost).sum * (t_settle_pq / brotherTotalPq)
    }
  } //分摊损耗总额
  lazy val realCosts: Array[Cost] = {
    if (costCut == null) {
      null
    } else {
      costCut.map(i => {
        if (brotherTotalPq == 0)
          i.cost = 0
        else
        i.cost = i.cost * (t_settle_pq / brotherTotalPq)
        i
      })
    }
  }
  var wastageCut: Array[Wastage] = _ //摊到用户身上的损耗
  var brotherTotalPq: BigDecimal = _
  lazy val totalWastage: BigDecimal = {
    if (wastageCut == null) {
      null
    } else {
      if (brotherTotalPq == 0)
        0
      else
        wastageCut.map(_.wastage).sum * (t_settle_pq / brotherTotalPq)
    }
  } //分摊损耗总额
  lazy val realWastages: Array[Wastage] = {
    if (wastageCut == null) {
      null
    } else {
      wastageCut.map(i => {
        if (brotherTotalPq == 0)
          i.wastage = 0
        else
          i.wastage = i.wastage * (t_settle_pq / brotherTotalPq)
        i
      })
    }
  }

  def this(u: user_info) = {
    this()
    cons_id = u.cons_id
    cons_name=u.cons_name
    cons_no = u.cons_no
    pub_priv_flag = u.pub_priv_flag
  }
}

object User  {
  def getRdd(ymd:String): RDD[User] = {
    val hc=SplitEntrance.hc
    val rdd = user_info.getHiveRdd(hc).map(i => new User(i))
    val tq_cons_pqrdd = tq_cons_pq.getHiveRdd(hc).filter(_.updatetime==ymd).map(i => (i.cons_id.toString, i))
    rdd.groupBy(_.cons_id.toString()).join(tq_cons_pqrdd).map(r => {
      val users = r._2._1
      val pq = r._2._2
      users.map(u => {
        u.t_settle_pq = pq.t_settle_pq
        u.tq_id = pq.tq_id
        u.updateTime=pq.updatetime
        u
      })
    }).flatMap(x => x).groupBy(_.tq_id).map(i => {
      val result = i._2
      val brotherTotalPq = result.map(_.t_settle_pq).sum
      result.map(r => {
        r.brotherTotalPq = brotherTotalPq
        r
      })
    }).flatMap(x => x)
  }
}
