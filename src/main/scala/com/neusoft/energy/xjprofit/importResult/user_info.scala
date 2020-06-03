package com.neusoft.energy.xjprofit.importResult

import com.neusoft.energy.xjprofit.SplitEntrance.sqlc
import com.neusoft.energy.xjprofit.fileRead._
import com.neusoft.energy.xjprofit.mytrait.{DataImportResult, HiveTable}
import com.neusoft.energy.xjprofit.tools.Tools
import org.apache.spark.rdd.RDD

case class user_info(
                    cons_id:Long, //用户ID
                    cons_no:String, //用户编号
                    cons_name:String, //用户名称
                    pub_priv_flag:String //用电类别（01公变，02专变）
                    )
object user_info extends HiveTable[user_info] with DataImportResult[user_info]{
  override def runSQL: RDD[user_info] = {
    g_tran.createView()
    high_user.createView()
    r_plan.createView()
    arc_e_cons_snap.createView()
    public_tq_dl.createView()
    high_user_dl.createView()
    val result: RDD[user_info] =sqlc.sql(Tools.getSql(tableName)).map(newObjectFromRow[user_info](_))
//    println("user_info Size:"+result.count())
    result
  }

}
