package com.neusoft.energy.xjprofit.topology
/**
 * @author zhanghengyuan
 * @note 抄表
 * @since 2019/9/23
 */
class Check {
  var cons_id:BigInt=_//用户id
  var mr_day:BigInt=_//抄表例日
  /**
   * @author zhujunwei
   * @note 获取用户抄表例日内的分摊损耗
   * @since 2019/10/8
   */

  def getUserWastage():List[Wastage] = ???   //获取用户抄表例日内的分摊损耗

  def getUserCost:List[Cost] = ???      //获取用户抄表例日内的分摊成本

}
