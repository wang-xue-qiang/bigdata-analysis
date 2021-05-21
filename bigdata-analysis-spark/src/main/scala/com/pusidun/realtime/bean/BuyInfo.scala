package com.pusidun.realtime.bean

import java.util.Date


case class BuyInfo(
                    mid: String, //设备id
                    uid: String, //用户id
                    game_name: String, //游戏名称
                    vc: String, //版本
                    ar: String, //地区
                    price: Int, //价格
                    priceName: String, //描述
                    clientTime:String, //订单时间
                    ts: Date //时间戳
                  ) {}