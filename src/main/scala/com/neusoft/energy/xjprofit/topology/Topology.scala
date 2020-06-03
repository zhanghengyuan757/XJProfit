package com.neusoft.energy.xjprofit.topology

import com.neusoft.energy.xjprofit.SplitEntrance.sc
import com.neusoft.energy.xjprofit.tools.Tools
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
 * @author zhanghengyuan
 * @note 拓扑网络
 * @since 2019/9/23
 */
class Topology extends Serializable {
  @transient lazy val log: Logger = Logger.getLogger(this.getClass)
  var topology: Array[Node] = _ //自顶向下的拓扑DAG
  lazy val topologyPercent: Array[(Node, BigDecimal)] = {
    topology.map(n => (n, getNodeSplitPercent(n)))
  } //自顶向下的拓扑DAG,对应到叶子的分摊比率汇总
  var tops: Array[Node] = _ //顶级电站
  var bottom: Node = _ //底层台区或计量箱
  var links: Array[(String, Array[String])] = _ //拓扑DAG的邻接表

  def init(psbid: String, node: Array[Node]): Unit = {
    topology = node.filter(_.sbid == psbid) //加载用户上级台区
    if (topology.nonEmpty) {
      bottom = topology.head
      links = topology.map(t => (t.sbid, t.psbidList))
      var pNodes = node.filter(n => {
        topology.flatMap(_.psbidList).filter(_ != null).contains(n.sbid)
      })
      pNodes=washPNodes(pNodes)
      var hasp = pNodes.nonEmpty
      var i = 0
      while (hasp) {
        i += 1
        topology ++= pNodes
        val pNodes2 = washPNodes(node.filter(n => {
          pNodes.flatMap(_.psbidList).filter(_.nonEmpty).contains(n.sbid)
        }))
        hasp = pNodes2.nonEmpty
        pNodes = pNodes2
        hasp = pNodes.nonEmpty
        links = (topology ++ pNodes2).map(t => (t.sbid, t.psbidList)).distinct
        val circle = getTopoCircle //TODO 影响性能
        if (circle.length != 0) {
          hasp = false
          log.error("拓扑数据存在闭环:\n" + (topology ++ pNodes2).filter(t => circle.map(_._1).contains(t.sbid))
            .map(i=>(i.sbid,i.psbidList.toString).toString()).mkString("\n"))
          topology = null
          return
        }
      }
      topology = topology.distinct.reverse
      tops = topology.filter(t => StringUtils.isBlank(t.psbid))
    }
  }
  /**
   * @author zhujunwei
   * @note 自底向上的清洗闭环
   * @since 2019/10/24
   */
  def washPNodes(pNodes:Array[Node]):Array[Node]={
    val washedPids =pNodes.map(i=>(i,Tools.voltLevelMap(i.voltLevel)))
      .filter(i=>{
        val pChildren=i._1.getchildInTopo(topology)
        pChildren.map(n=>Tools.voltLevelMap(n.voltLevel)).max<= i._2&&
          i._1.psbidList.intersect(pChildren.map(_.sbid)).length==0//去掉两点闭环
      }).map(_._1.sbid)
    topology.foreach(n=>{
      n.psbidList=n.psbidList.filter(washedPids.contains(_))
    })//清理掉本级记录的不合理的上一级
    pNodes.filter(n=>washedPids.contains(n.sbid))//清理掉不合理的上一级
  }
  def this(user: User, node: Array[Node]) = {
    this()
    init(user.tq_id, node)
  }

  def this(zone: Zone, node: Array[Node]) = {
    this()
    init(zone.ssxl, node)
  }

  /**
   * @author zhanghengyuan
   * @note 判断当前拓扑是否包含闭环
   * @since 2019/10/10
   */
  def getTopoCircle: Array[(String, Array[String])] = {
    var tempLinks = links
    var e = tempLinks.flatMap(_._2).distinct //找到所有入度不为0的顶点
    var in0p = tempLinks.map(_._1).diff(e)
    var hasIn0p = in0p.nonEmpty //判断是否有入度为0的顶点
    while (hasIn0p) {
      tempLinks = tempLinks.filter(l => e.contains(l._1)) //将入度为0的顶点及其出线从邻接表中删除
      e = tempLinks.flatMap(_._2).distinct //找到所有入度不为0的顶点
      in0p = tempLinks.map(_._1).diff(e)
      hasIn0p = in0p.nonEmpty //判断是否有入度为0的顶点
    }
    tempLinks //若无环结果应为空
  }

  /**
   * @author zhanghengyuan
   * @note 获取摊到用户身上的全部线路损耗
   * @since 2019/9/29
   */
  def getWastageCut: Array[Wastage] = {
    topologyPercent.map(np => {
      val n = np._1
      val p = np._2
      val result = new Wastage(n) //分摊好的损耗
      result.wastage = n.wastage.wastage * p
      result
    })
  }

  def getCostCut: Array[Cost] = {
    topologyPercent.filter(_._1.costs != null).flatMap(np => {
      np._1.costs.map(c => {
        c.cost = c.cost * np._2
        c
      })
    })
  }

  /**
   * @author zhanghengyuan
   * @note 通过递归得到该节点到底层分摊到的比率
   * @since 2019/9/29
   */
  def getNodeSplitPercent(node: Node): BigDecimal = {
    val children = node.getchildInTopo(topology)
    if (children.isEmpty) {
      1
    } else {
      if (children.length == 1) {
        children.head.splitPercent * getNodeSplitPercent(children.head)
      } else {
        var sum = BigDecimal(0)
        for (c <- children) {
          sum += c.splitPercent * getNodeSplitPercent(c)
        }
        sum
      }
    }
  }
}

object Topology {

  /**
   * @author zhanghengyuan
   * @note 获取低压用户拓扑网
   * @since 2019/9/26
   */
  def getUserTopology(ymd: String): RDD[(User, Topology)] = {
    val user = User.getRdd(ymd)
    val node = Node.getRdd(ymd)
    val boadcastNode: Broadcast[Array[Node]] = sc.broadcast(node.collect()) //数据量大了可能会有问题
    user.map(u => (u, new Topology(u, boadcastNode.value))).filter(_._2.topology != null).map(t => {
      t._1.wastageCut = t._2.getWastageCut
      t._1.costCut = t._2.getCostCut
      t
    })
  }

  /**
   * @author zhanghengyuan
   * @note 获取台区用户拓扑网
   * @since 2019/9/26
   */
  def getZoneTopology(ymd: String): RDD[(Zone, Topology)] = {
    val zone = Zone.getRdd(ymd)
    val node = Node.getRdd(ymd).filter(_.sbtype != "台区")
    val boadcastNode: Broadcast[Array[Node]] = sc.broadcast(node.collect()) //数据量大了可能会有问题
    zone.map(z => (z, new Topology(z, boadcastNode.value))).filter(_._2.topology != null).map(t => {
      t._1.wastageCut = t._2.getWastageCut
      t._1.costCut = t._2.getCostCut
      t
    })
  }
}
