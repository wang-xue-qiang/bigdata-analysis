package com.pusidun.realtime.bean

/**
  * Desc: 用于映射用户状态表的样例类
  */
case class UserStatus(
                       userId:String,  //用户id
                       ifConsumed:String //是否消费过   0首单   1非首单
                     )

