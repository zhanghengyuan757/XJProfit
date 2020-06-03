package com.neusoft.energy.xjprofit.result

import com.neusoft.energy.xjprofit.mytrait.HiveWritable
import com.neusoft.energy.xjprofit.topology.{Topology, User}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import com.neusoft.energy.xjprofit.SplitEntrance.sqlc
@deprecated
case class loss_detail(
   var id: String, //流水id
   sbid: String, //设备id
   sbmc: String, //设备名称
   sbtype: String, //设备类型
   loss: BigDecimal, //损耗
   cons_no: String, //用户id
   updatetime: String //更改时间
)
@deprecated
object loss_detail extends HiveWritable{
  override val tableName: String = "loss_detail"
  def getDf(topology: RDD[(User, Topology)]):DataFrame={
    import sqlc.implicits._
//    val rowCount=getRowCount
    topology.map(u=>{
      val user =u._1
      val cons_no=user.cons_no
      user.realWastages.map(w=>loss_detail(null,w.sbid,w.sbmc,w.sbtype,w.wastage,cons_no,w.ymd))
    }
    ).flatMap(x=>x)
//    .zipWithIndex().map(x=>{
//    val r=x._1
//    r.id=(x._2+1+rowCount).toString
//    r})
      .toDF()
  }
}