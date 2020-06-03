package com.neusoft.energy.xjprofit.importResult

import com.neusoft.energy.xjprofit.mytrait.HiveTable

case class equipment_cost(
    no: String, //流水
    ym: String, //年月yyyymm
    cost: BigDecimal, //金额
    name: String, //名称
    equip_id: String //设备编号
)
object  equipment_cost extends HiveTable[equipment_cost]
