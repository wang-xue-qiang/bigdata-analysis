package com.pusidun.dw;

import java.io.UnsupportedEncodingException;
import java.util.Random;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.pusidun.dw.bean.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 日志行为数据模拟
 */
public class AppMain {

    private final static Logger logger = LoggerFactory.getLogger(AppMain.class);
    private static Random rand = new Random();

    // 设备id
    private static int s_mid = 0;

    // 用户id
    private static int s_uid = 0;

    // 商品id
    private static int s_goodsid = 0;

    public static void main(String[] args) {

        // 参数一：控制发送每条的延时时间，默认是0
        Long delay = args.length > 0 ? Long.parseLong(args[0]) : 0L;

        // 参数二：循环遍历次数
        int loop_len = args.length > 1 ? Integer.parseInt(args[1]) : 1000;

        // 生成数据
        generateLog(delay, loop_len);
    }

    /**
     * 主程序
     */
    private static void generateLog(Long delay, int loop_len) {

        for (int i = 0; i < loop_len; i++) {

            int flag = rand.nextInt(2);

            switch (flag) {
                case (0):
                    //应用启动
                    AppStart appStart = generateStart();
                    String jsonString = JSON.toJSONString(appStart);

                    //控制台打印
                    logger.info(jsonString);
                    break;

                case (1):
                    JSONObject json = new JSONObject();
                    json.put("ap", "app");
                    json.put("cm", generateComFields());
                    JSONArray eventsArray = new JSONArray();

                    // 1.签到
                    if (rand.nextBoolean()) {
                        eventsArray.add(generateSign());
                        json.put("et", eventsArray);
                    }

                    // 2.抽奖
                    if (rand.nextBoolean()) {
                        eventsArray.add(generateLuckDraw());
                        json.put("et", eventsArray);
                    }

                    // 3.排行榜
                    if (rand.nextBoolean()) {
                        eventsArray.add(generateRank());
                        json.put("et", eventsArray);
                    }

                    // 4.内购
                    if (rand.nextBoolean()) {
                        eventsArray.add(generateShopBuy());
                        json.put("et", eventsArray);
                    }

                    // 5.球皮肤
                    if (rand.nextBoolean()) {
                        eventsArray.add(generateBall());
                        json.put("et", eventsArray);
                    }
                    // 6.订阅
                    if (rand.nextBoolean()) {
                        eventsArray.add(generateDingYue());
                        json.put("et", eventsArray);
                    }
                    // 7.弹窗
                    if (rand.nextBoolean()) {
                        eventsArray.add(generateShopWindow());
                        json.put("et", eventsArray);
                    }

                    // 8.主线关卡
                    if (rand.nextBoolean()) {
                        eventsArray.add(generateStage());
                        json.put("et", eventsArray);
                    }

                    // 9.钻石
                    if (rand.nextBoolean()) {
                        eventsArray.add(generateDiamond());
                        json.put("et", eventsArray);
                    }

                    // 10.金币
                    if (rand.nextBoolean()) {
                        eventsArray.add(generateCoin());
                        json.put("et", eventsArray);
                    }


                    // 11.道具
                    if (rand.nextBoolean()) {
                        eventsArray.add(generateProp());
                        json.put("et", eventsArray);
                    }

                    // 12.礼物盒子
                    if (rand.nextBoolean()) {
                        eventsArray.add(generateGift());
                        json.put("et", eventsArray);
                    }

                    // 13.广告
                    if (rand.nextBoolean()) {
                        eventsArray.add(generateAd());
                        json.put("et", eventsArray);
                    }

                    // 14.活动
                    if (rand.nextBoolean()) {
                        eventsArray.add(generateActivity());
                        json.put("et", eventsArray);
                    }

                    // 15.Ugc
                    if (rand.nextBoolean()) {
                        eventsArray.add(generateUGC());
                        json.put("et", eventsArray);
                    }

                    // 16.消息通知
                    if (rand.nextBoolean()) {
                        eventsArray.add(generateNotification());
                        json.put("et", eventsArray);
                    }


                    // 17.用户后台活跃
                    if (rand.nextBoolean()) {
                        eventsArray.add(generateBackground());
                        json.put("et", eventsArray);
                    }

                    //18.故障日志
                    if (rand.nextBoolean()) {
                        eventsArray.add(generateError());
                        json.put("et", eventsArray);
                    }

                    //时间
                    long millis = System.currentTimeMillis();

                    //控制台打印
                    logger.info(millis + "|" + json.toJSONString());
                    break;
            }

            // 延迟
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 公共字段设置
     */
    private static JSONObject generateComFields() {

        AppBase appBase = new AppBase();

        //设备id
        appBase.setMid(s_mid + "");
        s_mid++;

        // 用户id
        appBase.setUid(s_uid + "");
        s_uid++;

        //系统分组
        appBase.setGroupId(rand.nextInt(101) + "");

        // 程序版本号 5,6等
        appBase.setVc("" + rand.nextInt(20));

        //程序版本名 v1.1.1
        appBase.setVn("1." + rand.nextInt(4) + "." + rand.nextInt(10));

        // 安卓系统版本
        appBase.setOs("8." + rand.nextInt(3) + "." + rand.nextInt(10));

        // 语言  es,en,pt
        int flag = rand.nextInt(3);
        switch (flag) {
            case (0):
                appBase.setL("es");
                break;
            case (1):
                appBase.setL("en");
                break;
            case (2):
                appBase.setL("pt");
                break;
        }

        // 渠道号   从哪个渠道来的
        appBase.setSr(getRandomChar(1));

        // 区域
        flag = rand.nextInt(2);
        switch (flag) {
            case 0:
                appBase.setAr("BR");
            case 1:
                appBase.setAr("US");
        }

        // 手机品牌 ba ,手机型号 md，就取2位数字了
        flag = rand.nextInt(3);
        switch (flag) {
            case 0:
                appBase.setBa("Sumsung");
                appBase.setMd("sumsung-" + rand.nextInt(20));
                break;
            case 1:
                appBase.setBa("Huawei");
                appBase.setMd("Huawei-" + rand.nextInt(20));
                break;
            case 2:
                appBase.setBa("HTC");
                appBase.setMd("HTC-" + rand.nextInt(20));
                break;
        }

        // 嵌入sdk的版本
        appBase.setSv("V2." + rand.nextInt(10) + "." + rand.nextInt(10));
        // gmail
        appBase.setG(getRandomCharAndNumr(8) + "@gmail.com");

        // 屏幕宽高 hw
        flag = rand.nextInt(4);
        switch (flag) {
            case 0:
                appBase.setHw("640*960");
                break;
            case 1:
                appBase.setHw("640*1136");
                break;
            case 2:
                appBase.setHw("750*1134");
                break;
            case 3:
                appBase.setHw("1080*1920");
                break;
        }

        // 客户端产生日志时间
        long millis = System.currentTimeMillis();
        appBase.setT("" + (millis - rand.nextInt(99999999)));

        // 手机网络模式 3G,4G,WIFI
        flag = rand.nextInt(3);
        switch (flag) {
            case 0:
                appBase.setNw("3G");
                break;
            case 1:
                appBase.setNw("4G");
                break;
            case 2:
                appBase.setNw("WIFI");
                break;
        }

        // 拉丁美洲 西经34°46′至西经117°09；北纬32°42′至南纬53°54′
        // 经度
        appBase.setLn((-34 - rand.nextInt(83) - rand.nextInt(60) / 10.0) + "");
        // 纬度
        appBase.setLa((32 - rand.nextInt(85) - rand.nextInt(60) / 10.0) + "");

        return (JSONObject) JSON.toJSON(appBase);
    }


    /**
     * 启动日志
     */
    private static AppStart generateStart() {

        AppStart appStart = new AppStart();

        //设备id
        appStart.setMid(s_mid + "");
        s_mid++;

        // 用户id
        appStart.setUid(s_uid + "");
        s_uid++;

        // 程序版本号 5,6等
        appStart.setVc("" + rand.nextInt(20));

        //程序版本名 v1.1.1
        appStart.setVn("1." + rand.nextInt(4) + "." + rand.nextInt(10));

        // 安卓系统版本
        appStart.setOs("8." + rand.nextInt(3) + "." + rand.nextInt(10));

        //设置日志类型
        appStart.setEn("start");

        //    语言  es,en,pt
        int flag = rand.nextInt(3);
        switch (flag) {
            case (0):
                appStart.setL("es");
                break;
            case (1):
                appStart.setL("en");
                break;
            case (2):
                appStart.setL("pt");
                break;
        }

        // 渠道号   从哪个渠道来的
        appStart.setSr(getRandomChar(1));

        // 区域
        flag = rand.nextInt(2);
        switch (flag) {
            case 0:
                appStart.setAr("BR");
            case 1:
                appStart.setAr("MX");
        }

        // 手机品牌 ba ,手机型号 md，就取2位数字了
        flag = rand.nextInt(3);
        switch (flag) {
            case 0:
                appStart.setBa("Sumsung");
                appStart.setMd("sumsung-" + rand.nextInt(20));
                break;
            case 1:
                appStart.setBa("Huawei");
                appStart.setMd("Huawei-" + rand.nextInt(20));
                break;
            case 2:
                appStart.setBa("HTC");
                appStart.setMd("HTC-" + rand.nextInt(20));
                break;
        }

        // 嵌入sdk的版本
        appStart.setSv("V2." + rand.nextInt(10) + "." + rand.nextInt(10));
        // gmail
        appStart.setG(getRandomCharAndNumr(8) + "@gmail.com");

        // 屏幕宽高 hw
        flag = rand.nextInt(4);
        switch (flag) {
            case 0:
                appStart.setHw("640*960");
                break;
            case 1:
                appStart.setHw("640*1136");
                break;
            case 2:
                appStart.setHw("750*1134");
                break;
            case 3:
                appStart.setHw("1080*1920");
                break;
        }

        // 客户端产生日志时间
        long millis = System.currentTimeMillis();
        appStart.setT("" + (millis - rand.nextInt(99999999)));

        // 手机网络模式 3G,4G,WIFI
        flag = rand.nextInt(3);
        switch (flag) {
            case 0:
                appStart.setNw("3G");
                break;
            case 1:
                appStart.setNw("4G");
                break;
            case 2:
                appStart.setNw("WIFI");
                break;
        }

        // 拉丁美洲 西经34°46′至西经117°09；北纬32°42′至南纬53°54′
        // 经度
        appStart.setLn((-34 - rand.nextInt(83) - rand.nextInt(60) / 10.0) + "");
        // 纬度
        appStart.setLa((32 - rand.nextInt(85) - rand.nextInt(60) / 10.0) + "");

        //昵称
        appStart.setNickName(getCONTENT());
        //头像
        appStart.setPhotoIndex(rand.nextInt(23) + 1);
        //头像框
        appStart.setPhotoFrameIndex(rand.nextInt(10) + 1);
        //签到天数
        appStart.setSignDay(rand.nextInt(10) + 1);
        //钻石
        appStart.setDiamond(rand.nextInt(10000) + 1);
        //金币
        appStart.setCoin(rand.nextInt(3000) + 1);
        //主线关卡
        appStart.setNormalLevel(rand.nextInt(6000) + 1);
        return appStart;
    }

    /**
     * 1.签到数据
     */
    private static Object generateSign() {
        AppSign sign = new AppSign();
        int flag = rand.nextInt(1) + 1;
        sign.setAction(flag + "");// 0直接领取；1看广告领取
        flag = rand.nextInt(10) + 1;
        sign.setSignDay(flag);
        JSONObject jsonObject = (JSONObject) JSON.toJSON(sign);
        return packEventJson("sign", jsonObject);
    }

    /**
     * 2.抽奖
     */
    private static Object generateLuckDraw() {
        AppLuckDraw luckDraw = new AppLuckDraw();
        String[] kinds = {"diamond", "energy", "ball", "gold", "feiDie"};
        Integer[] diamondKinds = {30, 70, 120};
        Integer[] energyKinds = {4, 8};
        String kind = kinds[rand.nextInt(kinds.length)];
        luckDraw.setAdRewardKind(kind);
        if ("diamond".equals(kind)) {
            luckDraw.setAdReward(diamondKinds[rand.nextInt(diamondKinds.length)]);
        } else if ("energy".equals(kind)) {
            luckDraw.setAdReward(energyKinds[rand.nextInt(energyKinds.length)]);
        } else {
            luckDraw.setAdReward(1);
        }


        JSONObject jsonObject = (JSONObject) JSON.toJSON(luckDraw);
        return packEventJson("luckDraw", jsonObject);
    }

    /**
     * 3.点击排行榜
     */
    private static Object generateRank() {
        AppClickRank rank = new AppClickRank();
        int flag = rand.nextInt(2) + 1;
        rank.setAction(flag + "");
        JSONObject jsonObject = (JSONObject) JSON.toJSON(rank);
        return packEventJson("clickRank", jsonObject);
    }

    /**
     * 4.内购
     */
    private static Object generateShopBuy() {
        AppShopBuy shopBuy = new AppShopBuy();
        String[] kinds = {"diamond", "removeAd", "gift", "ad"};
        String[] diamondKinds = {"$0.99", "$2.99", "$4.99", "$9.99", "$29.99", "$49.99", "$99.99"};
        String[] giftKinds = {"$2.99", "$9.99"};

        String kind = kinds[rand.nextInt(kinds.length)];
        shopBuy.setAction(kind);

        if ("ad".equals(kinds)) {
            shopBuy.setBuyKind("ad");
            shopBuy.setDiamond(20);
        } else if ("removeAd".equals(kinds)) {
            shopBuy.setBuyKind("$2.99");
        } else if ("gift".equals(kinds)) {
            String giftKind = giftKinds[rand.nextInt(giftKinds.length)];
            shopBuy.setBuyKind(giftKind);
            if ("$2.99".equals(giftKind)) {
                shopBuy.setDiamond(540);
                shopBuy.setEnergy(50);
                shopBuy.setBallList("1");
            } else {
                shopBuy.setDiamond(800);
                shopBuy.setPropList("1,1,1,1,1,1,1,1");
            }

        } else {
            String diamondKind = diamondKinds[rand.nextInt(diamondKinds.length)];
            shopBuy.setBuyKind(diamondKind);
            if ("$0.99".equals(diamondKind)) {
                shopBuy.setDiamond(100);
            } else if ("$2.99".equals(diamondKind)) {
                shopBuy.setDiamond(300);
            } else if ("$4.99".equals(diamondKind)) {
                shopBuy.setDiamond(580);
            } else if ("$9.99".equals(diamondKind)) {
                shopBuy.setDiamond(1250);
            } else if ("$29.99".equals(diamondKind)) {
                shopBuy.setDiamond(3900);
            } else if ("$49.99".equals(diamondKind)) {
                shopBuy.setDiamond(6750);
            } else if ("$99.99".equals(diamondKind)) {
                shopBuy.setDiamond(15500);
            }
        }
        JSONObject jsonObject = (JSONObject) JSON.toJSON(shopBuy);
        return packEventJson("shopBuy", jsonObject);
    }

    /**
     * 5.球皮肤
     */
    private static Object generateBall() {
        AppBall ball = new AppBall();
        String[] kinds = {"diamond", "coin", "ad"};
        Integer[] ballList = {
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33
        };
        String kind = kinds[rand.nextInt(kinds.length)];
        ball.setAction(kind);
        if ("ad".equals(kind)) {
            ball.setBall(ballList[rand.nextInt(ballList.length)]);
        } else if ("coin".equals(kind)) {
            Integer buyBall = ballList[rand.nextInt(ballList.length)];
            if (buyBall >= 1 && buyBall <= 13) {
                ball.setNum(600);
            } else if (buyBall >= 14 && buyBall <= 20) {
                ball.setNum(660);
            } else if (buyBall >= 21 && buyBall <= 33) {
                ball.setNum(750);
            }
        } else if ("diamond".equals(kind)) {
            Integer buyBall = ballList[rand.nextInt(ballList.length)];
            if (buyBall >= 1 && buyBall <= 13) {
                ball.setNum(400);
            } else if (buyBall >= 14 && buyBall <= 20) {
                ball.setNum(440);
            } else if (buyBall >= 21 && buyBall <= 33) {
                ball.setNum(500);
            }
        }
        JSONObject jsonObject = (JSONObject) JSON.toJSON(ball);
        return packEventJson("ball", jsonObject);
    }

    /**
     * 6.订阅
     */
    private static Object generateDingYue() {
        AppDingYue dingYue = new AppDingYue();
        int flag = rand.nextInt(2) + 1;
        dingYue.setAction(flag + "");
        JSONObject jsonObject = (JSONObject) JSON.toJSON(dingYue);
        return packEventJson("dingYue", jsonObject);
    }

    /**
     * 7.弹窗
     */
    private static Object generateShopWindow() {
        AppShopWindow shopWindow = new AppShopWindow();
        int flag = rand.nextInt(10) + 1;
        shopWindow.setAction(flag + "");
        flag = rand.nextInt(20) + 1;
        shopWindow.setBuyKind(flag + "");
        JSONObject jsonObject = (JSONObject) JSON.toJSON(shopWindow);
        return packEventJson("shopWindow", jsonObject);
    }

    /**
     * 8.主线关卡
     */
    private static Object generateStage() {
        AppStage stage = new AppStage();
        int level = rand.nextInt(6000) + 1;
        stage.setLevel(level);
        stage.setMaxLevel(level);
        stage.setResult(rand.nextInt(2) + 1);
        JSONObject jsonObject = (JSONObject) JSON.toJSON(stage);
        return packEventJson("stage", jsonObject);
    }

    /**
     * 9.钻石
     */
    private static Object generateDiamond() {
        AppDiamond diamond = new AppDiamond();

        String[] kinds = {"add", "reduce"};
        String[] addConditions = {"sign", "luckDraw", "free", "buyDiamond"};
        Integer[] signkinds = {50, 80, 100};
        Integer[] luckDrawkinds = {30, 70, 120};
        Integer[] freekinds = {20};
        Integer[] buyDiamondkinds = {100, 300, 540, 580, 800, 1250, 3900, 6750, 15500};
        String[] reduceConditions = {"buyBall", "buyProp", "buyEnergy"};
        Integer[] buyBallkinds = {320, 350, 400, 440, 500, 660};
        Integer[] buyPropkinds = {50, 100};
        Integer[] buyEnergykinds = {50, 80, 60};


        String kind = kinds[rand.nextInt(kinds.length)];
        diamond.setAction(kind);

        if ("add".equals(kind)) {
            String condition = addConditions[rand.nextInt(addConditions.length)];
            diamond.setCondition(condition);
            if ("sign".equals(condition)) {
                int num = signkinds[rand.nextInt(signkinds.length)];
                diamond.setNum(num);
            } else if ("luckDraw".equals(condition)) {
                int num = luckDrawkinds[rand.nextInt(luckDrawkinds.length)];
                diamond.setNum(num);
            } else if ("free".equals(condition)) {
                diamond.setNum(20);
            } else if ("buyDiamond".equals(condition)) {
                int num = buyDiamondkinds[rand.nextInt(buyDiamondkinds.length)];
                diamond.setNum(num);
            }
        } else {
            String condition = reduceConditions[rand.nextInt(reduceConditions.length)];
            diamond.setCondition(condition);
            if ("buyBall".equals(condition)) {
                int num = buyBallkinds[rand.nextInt(buyBallkinds.length)];
                diamond.setNum(num);
            } else if ("buyProp".equals(condition)) {
                int num = buyPropkinds[rand.nextInt(buyPropkinds.length)];
                diamond.setNum(num);
            } else if ("buyEnergy".equals(condition)) {
                int num = buyEnergykinds[rand.nextInt(buyEnergykinds.length)];
                diamond.setNum(num);
            }
        }
        JSONObject jsonObject = (JSONObject) JSON.toJSON(diamond);
        return packEventJson("diamond", jsonObject);
    }

    /**
     * 10.金币产出
     */
    private static Object generateCoin() {
        AppCoin coin = new AppCoin();
        String[] conditions = {"buyBall", "gameOver"};
        String condition = conditions[rand.nextInt(conditions.length)];
        Integer[] ballList = {
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33
        };
        coin.setCondition(condition);
        if ("buyBall".equals(condition)) {
            coin.setAction("reduce");
            Integer buyBall = ballList[rand.nextInt(ballList.length)];
            coin.setBall(buyBall + "");
            if (buyBall >= 1 && buyBall <= 13) {
                coin.setNum(600);
            } else if (buyBall >= 14 && buyBall <= 20) {
                coin.setNum(660);
            } else if (buyBall >= 21 && buyBall <= 33) {
                coin.setNum(750);
            }
        } else {
            coin.setAction("add");
            coin.setNum(rand.nextInt(30) + 1);
        }
        JSONObject jsonObject = (JSONObject) JSON.toJSON(coin);
        return packEventJson("coin", jsonObject);
    }

    /**
     * 11.道具
     */
    private static Object generateProp() {
        AppProp prop = new AppProp();
        String[] kinds = {"add", "reduce"};
        String[] conditions = {"buy", "gameUse"};
        String kind = kinds[rand.nextInt(kinds.length)];
        prop.setAction(kind);
        if ("reduce".equals(kind)) {
            prop.setCondition("gameUse" + rand.nextInt(7) + 1);
            prop.setNum(1);
        } else {
            prop.setCondition("buy" + rand.nextInt(7) + 1);
            prop.setNum(1);
        }
        JSONObject jsonObject = (JSONObject) JSON.toJSON(prop);
        return packEventJson("prop", jsonObject);
    }

    /**
     * 12.礼物盒子
     */
    private static Object generateGift() {
        AppGift gift = new AppGift();
        String[] kinds = {"buy", "free", "ad"};
        String kind = kinds[rand.nextInt(kinds.length)];
        gift.setAction(kind);
        gift.setLevel(rand.nextInt(6000) + 1);
        JSONObject jsonObject = (JSONObject) JSON.toJSON(gift);
        return packEventJson("gift", jsonObject);
    }

    /**
     * 13.广告
     */
    private static Object generateAd() {
        AppAd ad = new AppAd();
        String[] kinds = {"ad", "video"};
        String kind = kinds[rand.nextInt(kinds.length)];
        ad.setAction(kind);
        if ("video".equals(kind)) {
            ad.setResult(rand.nextInt(1) + 1);
        }
        JSONObject jsonObject = (JSONObject) JSON.toJSON(ad);
        return packEventJson("ad", jsonObject);
    }

    /**
     * 14.活动
     */
    private static Object generateActivity() {
        AppActivity activity = new AppActivity();
        String[] kinds = {"icySummer", "deer", "bigMap"};
        String kind = kinds[rand.nextInt(kinds.length)];
        activity.setAction(kind);
        activity.setLevel(rand.nextInt(30) + 1);
        activity.setResult(rand.nextInt(1) + 1);
        JSONObject jsonObject = (JSONObject) JSON.toJSON(activity);
        return packEventJson("activity", jsonObject);
    }

    /**
     * 15.UGC
     */
    private static Object generateUGC() {
        AppUgcComment ugcComment = new AppUgcComment();
        String[] kinds = {"new", "hot"};
        String kind = kinds[rand.nextInt(kinds.length)];
        ugcComment.setAction(kind);
        ugcComment.setUgcId(rand.nextInt(20000));
        ugcComment.setResult(rand.nextInt(1) + 1);
        JSONObject jsonObject = (JSONObject) JSON.toJSON(ugcComment);
        return packEventJson("ugcComment", jsonObject);
    }

    /**
     * 16.消息通知
     */
    private static JSONObject generateNotification() {
        AppNotification appNotification = new AppNotification();
        int flag = rand.nextInt(4) + 1;
        // 动作
        appNotification.setAction(flag + "");
        // 通知id
        flag = rand.nextInt(4) + 1;
        appNotification.setType(flag + "");
        // 客户端弹时间
        appNotification.setAp_time((System.currentTimeMillis() - rand.nextInt(99999999)) + "");
        // 备用字段
        appNotification.setContent("");
        JSONObject jsonObject = (JSONObject) JSON.toJSON(appNotification);
        return packEventJson("notification", jsonObject);
    }

    /**
     * 17.后台活跃
     */
    private static JSONObject generateBackground() {
        AppBackground background = new AppBackground();
        // 启动源
        int flag = rand.nextInt(3) + 1;
        background.setActive_source(flag + "");
        JSONObject jsonObject = (JSONObject) JSON.toJSON(background);
        return packEventJson("background", jsonObject);
    }

    /**
     * 18.错误日志数据
     */
    private static JSONObject generateError() {

        AppError appErrorLog = new AppError();

        String[] errorBriefs = {"at cn.lift.dfdf.web.AbstractBaseController.validInbound(AbstractBaseController.java:72)", "at cn.lift.appIn.control.CommandUtil.getInfo(CommandUtil.java:67)"};        //错误摘要
        String[] errorDetails = {"java.lang.NullPointerException\\n    " + "at cn.lift.appIn.web.AbstractBaseController.validInbound(AbstractBaseController.java:72)\\n " + "at cn.lift.dfdf.web.AbstractBaseController.validInbound", "at cn.lift.dfdfdf.control.CommandUtil.getInfo(CommandUtil.java:67)\\n " + "at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\n" + " at java.lang.reflect.Method.invoke(Method.java:606)\\n"};        //错误详情

        //错误摘要
        appErrorLog.setErrorBrief(errorBriefs[rand.nextInt(errorBriefs.length)]);
        //错误详情
        appErrorLog.setErrorDetail(errorDetails[rand.nextInt(errorDetails.length)]);

        JSONObject jsonObject = (JSONObject) JSON.toJSON(appErrorLog);

        return packEventJson("error", jsonObject);
    }

    /**
     * 为各个事件类型的公共字段（时间、事件类型、Json数据）拼接
     */
    private static JSONObject packEventJson(String eventName, JSONObject jsonObject) {
        JSONObject eventJson = new JSONObject();
        eventJson.put("ett", (System.currentTimeMillis() - rand.nextInt(99999999)) + "");
        eventJson.put("en", eventName);
        eventJson.put("kv", jsonObject);
        return eventJson;
    }

    /**
     * 获取随机字母组合
     *
     * @param length 字符串长度
     */
    private static String getRandomChar(Integer length) {

        StringBuilder str = new StringBuilder();
        Random random = new Random();

        for (int i = 0; i < length; i++) {
            // 字符串
            str.append((char) (65 + random.nextInt(26)));// 取得大写字母
        }

        return str.toString();
    }

    /**
     * 获取随机字母数字组合
     *
     * @param length 字符串长度
     */
    private static String getRandomCharAndNumr(Integer length) {

        StringBuilder str = new StringBuilder();
        Random random = new Random();

        for (int i = 0; i < length; i++) {

            boolean b = random.nextBoolean();

            if (b) { // 字符串
                // int choice = random.nextBoolean() ? 65 : 97; 取得65大写字母还是97小写字母
                str.append((char) (65 + random.nextInt(26)));// 取得大写字母
            } else { // 数字
                str.append(String.valueOf(random.nextInt(10)));
            }
        }

        return str.toString();
    }

    /**
     * 生成单个汉字
     */
    private static char getRandomChar() {

        String str = "";
        int hightPos; //
        int lowPos;

        Random random = new Random();

        //随机生成汉子的两个字节
        hightPos = (176 + Math.abs(random.nextInt(39)));
        lowPos = (161 + Math.abs(random.nextInt(93)));

        byte[] b = new byte[2];
        b[0] = (Integer.valueOf(hightPos)).byteValue();
        b[1] = (Integer.valueOf(lowPos)).byteValue();

        try {
            str = new String(b, "GBK");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            System.out.println("错误");
        }

        return str.charAt(0);
    }

    /**
     * 拼接成多个汉字
     */
    private static String getCONTENT() {

        StringBuilder str = new StringBuilder();

        for (int i = 0; i < rand.nextInt(100); i++) {
            str.append(getRandomChar());
        }

        return str.toString();
    }

}
