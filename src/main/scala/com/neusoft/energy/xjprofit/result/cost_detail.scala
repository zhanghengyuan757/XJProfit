package com.neusoft.energy.xjprofit.result

import com.neusoft.energy.xjprofit.SplitEntrance.sqlc
import com.neusoft.energy.xjprofit.mytrait.HiveWritable
import com.neusoft.energy.xjprofit.topology.{Topology, User}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
@deprecated
case class cost_detail(
   var id: String, //流水id
   sbid: String, //设备id
   cost: BigDecimal, //成本金额
   costName: String, //成本名称
   cons_no: String, //用户id
   updatetime: String //更改时间
)
@deprecated
object cost_detail extends HiveWritable{
  override val tableName: String = "cost_detail"
  def getDf( topology: RDD[(User, Topology)]):DataFrame={
    import sqlc.implicits._
//    val rowCount=getRowCount
    topology.map(u=>{
      val user =u._1
      val cons_no=user.cons_no
      user.realCosts.map(w=>cost_detail(null,w.sbid,w.cost,w.costName,cons_no,w.ymd))
    }
    ).flatMap(x=>x)
//      .zipWithIndex().map(x=>{
//      val r=x._1
//      r.id=(x._2+1+rowCount).toString
//      r})
      .toDF()
  }
}
