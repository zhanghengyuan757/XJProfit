package com.neusoft.energy.xjprofit.topology

import com.neusoft.energy.xjprofit.importResult.equipment_cost
import com.neusoft.energy.xjprofit.tools.Tools

/**
 * @author zhanghengyuan
 * @note 成本
 * @since 2019/9/23
 */
case class Cost(
   sbid:String,//设备ID
   var cost:BigDecimal,//成本金额
   costName:String,//成本名称
   ymd:String//年月日 日期
){
  def this(c:equipment_cost){
    this(c.equip_id,c.cost/Tools.getMonthDaysByYm(c.ym),c.name,c.ym)
  }
}
