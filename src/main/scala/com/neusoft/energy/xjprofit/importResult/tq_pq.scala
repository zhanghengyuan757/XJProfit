package com.neusoft.energy.xjprofit.importResult

import com.neusoft.energy.xjprofit.mytrait.HiveTable

case class tq_pq(
      tq_id:String, //台区id
      tq_no:String,//台区No
      tq_name:String,//台区
      ssxl:String, //所属线路
      date: String, //更新时间
      t_settle_pq:BigDecimal //供电量（入）
  )

object tq_pq extends HiveTable[tq_pq]