package com.pusidun.realtime.bean

/**
  * Desc: 用户样例类
  */
case class UserInfo(
                     id:String,
                     user_level:String,
                     birthday:String,
                     gender:String,
                     var age_group:String,//年龄段
                     var gender_name:String) //性别

