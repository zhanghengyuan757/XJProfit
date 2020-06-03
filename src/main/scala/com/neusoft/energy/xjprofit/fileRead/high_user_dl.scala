package com.neusoft.energy.xjprofit.fileRead

import com.neusoft.energy.xjprofit.SplitEntrance.sqlc
import com.neusoft.energy.xjprofit.mytrait.TextLoadTable
@deprecated
case class high_user_dl(
                         CONS_NO :String,//客户编号
                         CONS_NAME :String,//客户名称
                         zxPower :BigDecimal,//正向电量
                         lineNo:String,//线路编号
                         lineName:String//线路名称
                       )
@deprecated
object high_user_dl extends TextLoadTable[high_user_dl]{
  override val path: String = "text/high_user_dl.txt"
  override def newObject(ss: String): high_user_dl = {
    val str = ss.split(",")
    var zxPower=str(2)
    if(zxPower.length==0){
      zxPower="0"
    }
    try {
    if(ss.endsWith(",")) {
      high_user_dl(str(0),str(1),BigDecimal(zxPower),str(3),null)
    }
    else
      high_user_dl(str(0),str(1),BigDecimal(zxPower),str(3),str(4))
    } catch {
      case e:Exception =>
        println("无法处理的数据："+str.mkString(",")+"，原数据："+ss)
        null
    }
  }
  override def createView():Unit= {
    import sqlc.implicits._
    getRdd.toDF().registerTempTable(tableName)
  }
}
