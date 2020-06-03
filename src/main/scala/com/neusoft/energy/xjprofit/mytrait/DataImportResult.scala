package com.neusoft.energy.xjprofit.mytrait

import org.apache.spark.rdd.RDD
@deprecated
trait DataImportResult[T] {
  def runSQL:RDD[T]
}
