package com.pusidun.dw.bean;

/**
 * 启动日志
 */
public class AppStart extends AppBase {
    private String fbAccount;    //	FaceBook账号
    private String fbName;    //	FaceBook名字
    private String fbPhotoUrl;    //	FaceBook头像
    private String fbSex;    //	FaceBook用户性别
    private String fbToken;    //	FaceBook的Token
    private String tokenEndDate;    //	FaceBook的Token的有效时间
    private String nickName;    //用户昵称
    private Integer photoIndex;    //	系统用户头像
    private Integer photoFrameIndex;    //	系统头像框
    private Integer signDay;    //	签到天数
    private Integer signDayState;    //	签到状态
    private String signDayDate;    //	签到日期
    private Integer diamond;    //	钻石数量
    private Integer coin;    //	金币数量
    private Integer energy;    //	体力
    private String ballList;    //	球皮肤列表
    private String propList;    //	道具列表
    private Integer gravityScore;    //	挑战之重力模式最高分数
    private Integer cleanScore;    //	挑战之清平模式最高分数
    private Integer infinityScore;    //	挑战之无线模式最高分数
    private Integer gravityLevel;    //	挑战之重力模式最大球数
    private Integer cleanLevel;    //	挑战之清平模式最大关卡数
    private Integer infinityLevel;    //	挑战之无线模式最大行数
    private Integer normalLevel;    //	主线当前关卡
    private String normalStar;    //	主线每关星星数
    private Integer normalTotalStar;    //	主线总星星数
    private Integer deerLevel;    //	活动之新技能块当前关卡
    private String deerStar;    //	活动之新技能块每关星星数
    private Integer deerCoin;    //	活动之新技能块金币奖励
    private String deerGift;    //	活动之新技能块奖励
    private Integer icySummerLevel;    //	活动之冰爽一夏当前关卡
    private String icySummerStar;    //	活动之冰爽一夏每关星星数
    private Integer icySummerReward;    //	活动之冰爽一夏奖励
    private String icySummerDiamond;    //	活动之冰爽一夏钻石
    private String bigMapData;    //	活动之超大地图数据
    private Integer iap;//	内购;
    private String en;//启动日志类型标记

    public String getEn() { return en; }

    public void setEn(String en) { this.en = en; }

    public String getNickName() {
        return nickName;
    }

    public void setNickName(String nickName) {
        this.nickName = nickName;
    }

    public Integer getPhotoIndex() {
        return photoIndex;
    }

    public void setPhotoIndex(Integer photoIndex) {
        this.photoIndex = photoIndex;
    }

    public Integer getPhotoFrameIndex() {
        return photoFrameIndex;
    }

    public void setPhotoFrameIndex(Integer photoFrameIndex) {
        this.photoFrameIndex = photoFrameIndex;
    }

    public Integer getSignDay() {
        return signDay;
    }

    public void setSignDay(Integer signDay) {
        this.signDay = signDay;
    }

    public Integer getSignDayState() {
        return signDayState;
    }

    public void setSignDayState(Integer signDayState) {
        this.signDayState = signDayState;
    }

    public String getSignDayDate() {
        return signDayDate;
    }

    public void setSignDayDate(String signDayDate) {
        this.signDayDate = signDayDate;
    }

    public Integer getDiamond() {
        return diamond;
    }

    public void setDiamond(Integer diamond) {
        this.diamond = diamond;
    }

    public Integer getCoin() {
        return coin;
    }

    public void setCoin(Integer coin) {
        this.coin = coin;
    }

    public Integer getEnergy() {
        return energy;
    }

    public void setEnergy(Integer energy) {
        this.energy = energy;
    }

    public String getBallList() {
        return ballList;
    }

    public void setBallList(String ballList) {
        this.ballList = ballList;
    }

    public String getPropList() {
        return propList;
    }

    public void setPropList(String propList) {
        this.propList = propList;
    }

    public Integer getGravityScore() {
        return gravityScore;
    }

    public void setGravityScore(Integer gravityScore) {
        this.gravityScore = gravityScore;
    }

    public Integer getCleanScore() {
        return cleanScore;
    }

    public void setCleanScore(Integer cleanScore) {
        this.cleanScore = cleanScore;
    }

    public Integer getInfinityScore() {
        return infinityScore;
    }

    public void setInfinityScore(Integer infinityScore) {
        this.infinityScore = infinityScore;
    }

    public Integer getGravityLevel() {
        return gravityLevel;
    }

    public void setGravityLevel(Integer gravityLevel) {
        this.gravityLevel = gravityLevel;
    }

    public Integer getCleanLevel() {
        return cleanLevel;
    }

    public void setCleanLevel(Integer cleanLevel) {
        this.cleanLevel = cleanLevel;
    }

    public Integer getInfinityLevel() {
        return infinityLevel;
    }

    public void setInfinityLevel(Integer infinityLevel) {
        this.infinityLevel = infinityLevel;
    }

    public Integer getNormalLevel() {
        return normalLevel;
    }

    public void setNormalLevel(Integer normalLevel) {
        this.normalLevel = normalLevel;
    }

    public String getNormalStar() {
        return normalStar;
    }

    public void setNormalStar(String normalStar) {
        this.normalStar = normalStar;
    }

    public Integer getNormalTotalStar() {
        return normalTotalStar;
    }

    public void setNormalTotalStar(Integer normalTotalStar) {
        this.normalTotalStar = normalTotalStar;
    }

    public Integer getDeerLevel() {
        return deerLevel;
    }

    public void setDeerLevel(Integer deerLevel) {
        this.deerLevel = deerLevel;
    }

    public String getDeerStar() {
        return deerStar;
    }

    public void setDeerStar(String deerStar) {
        this.deerStar = deerStar;
    }

    public Integer getDeerCoin() {
        return deerCoin;
    }

    public void setDeerCoin(Integer deerCoin) {
        this.deerCoin = deerCoin;
    }

    public String getDeerGift() {
        return deerGift;
    }

    public void setDeerGift(String deerGift) {
        this.deerGift = deerGift;
    }

    public Integer getIcySummerLevel() {
        return icySummerLevel;
    }

    public void setIcySummerLevel(Integer icySummerLevel) {
        this.icySummerLevel = icySummerLevel;
    }

    public String getIcySummerStar() {
        return icySummerStar;
    }

    public void setIcySummerStar(String icySummerStar) {
        this.icySummerStar = icySummerStar;
    }

    public Integer getIcySummerReward() {
        return icySummerReward;
    }

    public void setIcySummerReward(Integer icySummerReward) {
        this.icySummerReward = icySummerReward;
    }

    public String getIcySummerDiamond() {
        return icySummerDiamond;
    }

    public void setIcySummerDiamond(String icySummerDiamond) {
        this.icySummerDiamond = icySummerDiamond;
    }

    public String getBigMapData() {
        return bigMapData;
    }

    public void setBigMapData(String bigMapData) {
        this.bigMapData = bigMapData;
    }

    public Integer getIap() {
        return iap;
    }

    public void setIap(Integer iap) {
        this.iap = iap;
    }

    public String getFbAccount() {
        return fbAccount;
    }

    public void setFbAccount(String fbAccount) {
        this.fbAccount = fbAccount;
    }

    public String getFbName() {
        return fbName;
    }

    public void setFbName(String fbName) {
        this.fbName = fbName;
    }

    public String getFbPhotoUrl() {
        return fbPhotoUrl;
    }

    public void setFbPhotoUrl(String fbPhotoUrl) {
        this.fbPhotoUrl = fbPhotoUrl;
    }

    public String getFbSex() {
        return fbSex;
    }

    public void setFbSex(String fbSex) {
        this.fbSex = fbSex;
    }

    public String getFbToken() {
        return fbToken;
    }

    public void setFbToken(String fbToken) {
        this.fbToken = fbToken;
    }

    public String getTokenEndDate() {
        return tokenEndDate;
    }

    public void setTokenEndDate(String tokenEndDate) {
        this.tokenEndDate = tokenEndDate;
    }
}
