package com.neusoft.energy.xjprofit.mytrait

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

trait HiveTable[T] extends Reflective with Serializable {
  def getHiveRdd(hc: HiveContext)(implicit mf: scala.reflect.Manifest[T]): RDD[T] = {
    val dbName = databaseName
    hc.sql(s"select * from $dbName.$tableName").map(newObjectFromRow[T](_))
  }
}