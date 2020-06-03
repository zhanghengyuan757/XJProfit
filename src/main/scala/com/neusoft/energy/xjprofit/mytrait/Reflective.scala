package com.neusoft.energy.xjprofit.mytrait

import java.lang
import java.lang.reflect.{Constructor, Field}

import org.apache.spark.sql.Row

trait Reflective {
  val isCaseClass:Boolean=true
  val tableName: String = {
    val className = this.getClass.getSimpleName
    className.substring(0, className.length - 1)
  } //表名
  val databaseName = "xjprofit"

  /**
   * @note 按照字段类型转换数据格式
   * @since 2019/6/12
   * @author zhanghy
   */
  protected def getValueByFiled(filed: Field, value: String): Object = {
    if (null == value) {
      filed.getType.getSimpleName match {
        case "String" => null
        case "BigDecimal" => BigDecimal(0).asInstanceOf[Object]
        case "BigInt" => null
        case "boolean" => false.asInstanceOf[Object]
        case "int" => 0.asInstanceOf[Object]
        case "short" => 0.asInstanceOf[Object]
        case "long" => 0L.asInstanceOf[Object]
        case "double" => 0.toDouble.asInstanceOf[Object]
        case "float" => 0F.asInstanceOf[Object]
        case _ => null
      }
    } else {
      filed.getType.getSimpleName match {
        case "String" => value.asInstanceOf[Object]
        case "BigDecimal" => BigDecimal(value).asInstanceOf[Object]
        case "BigInt" => BigInt(value).asInstanceOf[Object]
        case "boolean" => value.toBoolean.asInstanceOf[Object]
        case "int" => value.toInt.asInstanceOf[Object]
        case "short" => value.toShort.asInstanceOf[Object]
        case "long" => value.toLong.asInstanceOf[Object]
        case "double" => value.toDouble.asInstanceOf[Object]
        case "float" => value.toFloat.asInstanceOf[Object]
        case _ => null
      }
    }
  }

  def getStringByFiledFromRow(filed: Field, row: Row, nul: String = null): String = {
    val fn = filed.getName
    val exist = row.schema.fieldNames.contains(fn)
    if (!exist || row.isNullAt(row.fieldIndex(fn))) {
      nul
    } else {
      row.getAs(row.fieldIndex(fn)).toString
    }
  }

  def getObjectByFiledFromRow(f: Field, r: Row, nul: String = null): Object = {
    val fn = f.getName
    val exist = r.schema.fieldNames.contains(fn)
    if (!exist || r.isNullAt(r.fieldIndex(fn))) {
      getValueByFiled(f, null)
    } else {
      getValueByFiled(f, r.getAs[Object](r.fieldIndex(fn)).toString)
    }
  }

  def getObjectByFiledFromMap(f: Field, m: Map[String, Object], nul: String = null): Object = {
    val fn = f.getName
    val exist = m.keySet.contains(fn)
    if (!exist || m.getOrElse(fn, null) == null) {
      getValueByFiled(f, null)
    } else {
      val value=m(fn)
      var vstr=value.toString
      value match {
        case double: lang.Double =>
          val bd = BigDecimal(double)
          if (BigDecimal(bd.toLong) == bd) {
            vstr = bd.toLong.toString
          }
        case _ =>
      }
      getValueByFiled(f,vstr)
    }
  }

  def setValueByFiledFromRow(f: Field, r: Row, nul: String = null): Unit = {
    val fn = f.getName
    getValueByFiled(f, r.getAs[Object](r.fieldIndex(fn)).toString)
  }

  def newObjectFromRow[T](r: Row)(implicit mf: scala.reflect.Manifest[T]): T = {
    val c = mf.runtimeClass
    var fs = c.getDeclaredFields
    if(isCaseClass){
      val init: Constructor[_] = c.getDeclaredConstructors.head
      val args= fs.map(f => getObjectByFiledFromRow(f, r))
      init.newInstance(args.toArray:_*).asInstanceOf[T]
    }else{
      val superc = c.getSuperclass
      if (superc != null) {
        fs ++= superc.getDeclaredFields
      }
      val init: Constructor[_] = c.getDeclaredConstructors.head
      val o = init.newInstance().asInstanceOf[T]
      fs.filter(f => {
        val fn = f.getName
        r.schema.fieldNames.contains(fn)
      }).foreach(f => {
        f.setAccessible(true)
        f.set(o, getObjectByFiledFromRow(f, r))
      })
      o
    }

  }

  def newObjectFromMap[T](m: Map[String, Object])(implicit mf: scala.reflect.Manifest[T]): T = {
    val c = mf.runtimeClass
    var fs = c.getDeclaredFields
    if(isCaseClass){
      val init: Constructor[_] = c.getDeclaredConstructors.head
      val args= fs.map(f => getObjectByFiledFromMap(f, m))
      init.newInstance(args.toArray:_*).asInstanceOf[T]
    }else {
      val superc = c.getSuperclass
      if (superc != null) {
        fs ++= superc.getDeclaredFields
      }
      val init: Constructor[_] = c.getDeclaredConstructors.head
      val o = init.newInstance().asInstanceOf[T]
      fs.filter(f => {
        val fn = f.getName
        m.keySet.contains(fn)
      }).foreach(f => {
        f.setAccessible(true)
        f.set(o, getObjectByFiledFromMap(f, m))
      })
      o
    }
  }
}
