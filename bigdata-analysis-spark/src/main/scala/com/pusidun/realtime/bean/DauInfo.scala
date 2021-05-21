package com.pusidun.realtime.bean

import java.util.Date

/**
 * 封装日活数据样例类
 */
case class DauInfo(
                    mid: String, //设备id
                    uid: String, //用户id
                    gameName: String, //游戏名称
                    ar: String, //地区
                    ch: String, //渠道
                    vc: String, //版本
                    dt: String, //日期
                    hr: String, //小时
                    ts: Date //时间戳
                  ) {}