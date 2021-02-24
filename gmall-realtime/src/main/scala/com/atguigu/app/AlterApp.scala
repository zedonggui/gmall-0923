package com.atguigu.app

object AlterApp {
  def main(args: Array[String]): Unit = {
    //1.创建sparkConf

    //2.创建StreamingContext

    //3.消费kafka数据

    //4.将数据转化成样例类(EventLog文档中有)，补充时间字段，将数据转换为（k，v） k->?  v->?

    //5.开窗5min

    //6.分组聚合按照mid

    //7.筛选数据，首先用户得领优惠券，并且用户没有浏览商品行为（将符合这些行为的uid保存下来至set集合）

    //8.生成预警日志(将数据保存至CouponAlertInfo样例类中，文档中有)，条件：符合第七步要求，并且uid个数>=3（主要为“过滤”出这些数据），实质：补全CouponAlertInfo样例类

    //9.将预警数据写入ES

    //10.开启


  }

}
