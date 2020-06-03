package com.neusoft.energy.xjprofit.topology

import org.apache.spark.rdd.RDD

/**
 * @author zhanghengyuan
 * @note 量费
 * @since 2019/9/23
 */
class Fee {
  var fee:BigDecimal=_//每月的电费
  var kWh:BigDecimal=_//每月用电量
  var ym:String=_//年月
  var cons_id:BigInt =_ //用户ID
  /**
   * @author zhujunwei
   * @note 获取每月的电费
   * @since 2019/9/29
   */

  def getKWh(check: RDD[Check],users:RDD[User]): BigDecimal ={
    fee = 0
    kWh = 0
    var checks = check.filter(_.cons_id == cons_id).collect().reverse.toList
    var nowDay = checks.head.mr_day   //当前日前
    var lastMonthDay = checks.tail.head.mr_day //上个月日期
    var usersTheMonth = users.filter(_.mr_day-lastMonthDay>=0).filter(nowDay-_.mr_day>0).collect().toList   //获取这一个月内的用户数据
    for(i <- usersTheMonth)
      kWh += i.t_settle_pq
    kWh
  }

  def getFee():BigDecimal = ???
}
