package com.neusoft.energy.xjprofit.topology

/**
 * @author zhanghengyuan
 * @note 损耗
 * @since 2019/9/23
 */
case class Wastage(
    sbid:String,//设备ID
    sbmc:String,//设备名称
    sbtype:String,//设备名称
    var wastage:BigDecimal, //设备类型
    ymd:String
){
  def this(n:Node)={
    this(n.sbid,n.sbmc,n.sbtype,n.in_settle_pq - n.out_settle_pq,n.updatetime)
  }
}




