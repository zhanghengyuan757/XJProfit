package com.neusoft.energy.xjprofit.importResult

import com.neusoft.energy.xjprofit.SplitEntrance.sqlc
import com.neusoft.energy.xjprofit.fileRead.{g_tran, high_user, high_user_dl, public_tq_dl}
import com.neusoft.energy.xjprofit.mytrait.{DataImportResult, HiveTable}
import com.neusoft.energy.xjprofit.tools.Tools
import org.apache.spark.rdd.RDD

case class tq_cons_pq (
    cons_id:String,//用户ID
    tq_id:String,//台区ID
    t_settle_pq:BigDecimal,//用户电量
    updatetime:String//更改时间
  )
object  tq_cons_pq extends HiveTable[tq_cons_pq] with DataImportResult[tq_cons_pq]{
  override def runSQL: RDD[tq_cons_pq] = {
    g_tran.createView()
    high_user.createView()
    high_user_dl.createView()
    public_tq_dl.createView()
    val result: RDD[tq_cons_pq] =sqlc.sql(Tools.getSql(tableName)).map(newObjectFromRow[tq_cons_pq](_))
//    println("tq_cons_pq Size:"+result.count())
    result
  }
}
