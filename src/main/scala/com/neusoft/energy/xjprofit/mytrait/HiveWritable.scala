package com.neusoft.energy.xjprofit.mytrait


import org.apache.spark.sql.{DataFrame, SaveMode}

trait HiveWritable {

  val tableName: String = {
    val className = this.getClass.getSimpleName
    className.substring(0, className.length - 1)
  } //表名
  val databaseName = "xjprofit"

//  def getRowCount: Long = {
//    val dbName = databaseName
//    hc.sql(s"select count(1) from $dbName.$tableName").head().getLong(0)
//  }

  def putRddToHive(df: DataFrame): Unit = {
    val dbName = databaseName
    df.write.mode(SaveMode.Overwrite)
      .saveAsTable(s"$dbName.$tableName")
  }
}
