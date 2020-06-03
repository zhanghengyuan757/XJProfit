package com.neusoft.energy.xjprofit.result

import com.neusoft.energy.xjprofit.SplitEntrance.sqlc
import com.neusoft.energy.xjprofit.mytrait.HiveWritable
import com.neusoft.energy.xjprofit.topology.{Topology, User}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.math.BigDecimal.RoundingMode
case class l_loss_view_cons(
        cons_no:String,
        cons_name:String,
        date:String,
        gmdcb:Float,//购电成本
        sdsr:Float,//售电成本
        gdcb:Float,//供电成本
        zhnysr:Float,//综合能源收入
        xmcb:Float,//项目成本
        fxmcb:Float,//非项目成本
        jxywcb:Float,//检修运维成本
        zjcb:Float,//折旧成本
        gdl:Float,//供电量
        sdl:Float,//售电量
        ftshdl:Float//分摊损耗电量
)
object l_loss_view_cons extends HiveWritable{
  def getDf( topology: RDD[(User, Topology)]):DataFrame={
    import sqlc.implicits._
    topology.map(u=>{
      val user =u._1
      l_loss_view_cons(
        user.cons_no,
        user.cons_name,
        user.updateTime,
        0,//购电成本
        (user.t_settle_pq*0.24).setScale(4,RoundingMode.HALF_UP).floatValue(),//售电成本
        ((user.t_settle_pq+user.totalWastage)*0.24)
          .setScale(4,RoundingMode.HALF_UP).floatValue(),//供电成本
        0,//综合能源收入
        0,//项目成本
        0,//非项目成本
        0,//检修运维成本
        user.totalCost.setScale(4,RoundingMode.HALF_UP).floatValue(),//折旧成本
        (user.t_settle_pq+user.totalWastage).setScale(4,RoundingMode.HALF_UP).floatValue(),//供电量
        user.t_settle_pq.setScale(4,RoundingMode.HALF_UP).floatValue(),//售电量
        user.totalWastage.setScale(4,RoundingMode.HALF_UP).floatValue())//分摊损耗电量
    }).toDF()
  }
}