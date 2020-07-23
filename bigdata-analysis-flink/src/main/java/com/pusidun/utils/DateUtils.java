package com.pusidun.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 常用时间工具类
 */
public class DateUtils {

    public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
    public final static String DATE_PATTERN = "yyyy-MM-dd";
    public final static String DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    public final static String DATE_TIME2_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSS";
    public final static String DATE_TIME3_PATTERN = "yyyy-MM-dd'T'HH:mm:ss";
    public final static String DATE_TIME4_PATTERN = "yyyy-MM-dd HH:mm";

    /**
     * 格式化日期
     * @param date
     * @return
     */
    public static String format(Date date) {
        return format(date, DATE_PATTERN);
    }

    /**
     * 按照指定格式格式化日期
     * @param date
     * @param pattern
     * @return
     */
    public static String format(Date date, String pattern) {
        if(date != null){
            SimpleDateFormat df = new SimpleDateFormat(pattern);
            return df.format(date);
        }
        return null;
    }

    /**
     * 计算出当前时间距离凌晨差值（秒数）
     * @throws Exception
     */
    public static long getOverTime() throws Exception{
        long now = System.currentTimeMillis();
        SimpleDateFormat sdfOne = new SimpleDateFormat(DATE_PATTERN);
        long overTime = (now - (sdfOne.parse(sdfOne.format(now)).getTime()))/1000;
        return overTime;
    }

    /**
     * 字符串转Date
     * @param DateStr
     * @return
     */
    public static Date Str2Date(String DateStr){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DATE_PATTERN);
        Date date = null;
        try {
            date = simpleDateFormat.parse(DateStr);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return date;
    }
    /**
     * 字符串转Date
     * @param DateStr
     * @return
     */
    public static Date Str2Date(String DateStr, String pattern){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        Date date = null;
        try {
            date = simpleDateFormat.parse(DateStr);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return date;
    }

    /**
     * 字符串转Date字符串
     * @param DateStr
     * @return
     */
    public static String Str2DateStr(String DateStr){
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DATE_TIME_PATTERN);
            return format(simpleDateFormat.parse(DateStr),DATE_TIME3_PATTERN);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }





    /**
     *  long类型转换成日期
     * @param lo 毫秒数
     * @return String yyyy-MM-dd HH:mm:ss
     */
    public static String longToDate(long lo){
        Date date = new Date(lo);
        SimpleDateFormat sd = new SimpleDateFormat(DATE_TIME_PATTERN);
        return sd.format(date);
    }

    /**
     * 字符串转时间秒为单位
     * @param DateStr
     * @return
     */
    public static  String str2Time(String DateStr){
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DATE_TIME_PATTERN);
            return  simpleDateFormat.parse(DateStr).getTime()/1000 +"";
        } catch (Exception e) {
            return DateStr;
        }
    }

    public static String getYearbasebyAge(String age) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.YEAR, -Integer.valueOf(age));
        Date newdate = calendar.getTime();
        DateFormat dateFormat = new SimpleDateFormat("yyyy");
        String newdatestring = dateFormat.format(newdate);
        Integer newdateinteger = Integer.valueOf(newdatestring);
        String yearbasetype = "未知";
        if (newdateinteger >= 1940 && newdateinteger < 1950) {
            yearbasetype = "40后";
        } else if (newdateinteger >= 1950 && newdateinteger < 1960) {
            yearbasetype = "50后";
        } else if (newdateinteger >= 1960 && newdateinteger < 1970) {
            yearbasetype = "60后";
        } else if (newdateinteger >= 1970 && newdateinteger < 1980) {
            yearbasetype = "70后";
        } else if (newdateinteger >= 1980 && newdateinteger < 1990) {
            yearbasetype = "80后";
        } else if (newdateinteger >= 1990 && newdateinteger < 2000) {
            yearbasetype = "90后";
        } else if (newdateinteger >= 2000 && newdateinteger < 2010) {
            yearbasetype = "00后";
        } else if (newdateinteger >= 2010) {
            yearbasetype = "10后";
        }
        return yearbasetype;
    }


    /**
     * 两个时间段差值
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return
     * @throws ParseException
     */
    public static int getDaysByStartAndEndTime(long startTime, long endTime) throws ParseException {
        Date start = new Date(startTime);
        Date end = new Date(endTime);
        Calendar startcalendar = Calendar.getInstance();
        Calendar endcalendar = Calendar.getInstance();
        startcalendar.setTime(start);
        endcalendar.setTime(end);
        int days = 0;
        while (startcalendar.before(endcalendar)) {
            startcalendar.add(Calendar.DAY_OF_YEAR, 1);
            days += 1;
        }
        return days;
    }


    public static String gethoursbydate(String timevalue) throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd hhmmss");
        Date time = dateFormat.parse(timevalue);
        dateFormat = new SimpleDateFormat("hh");
        String resulthour = dateFormat.format(time);
        return resulthour;
    }


    public static Date getDate(String timeStr) throws ParseException {
        return dateFormat.parse(timeStr);
    }

    /**
     * 格式化nginx时间
     * @param dateStr
     * @return
     */
    public static Long parseNginxTime(String dateStr){
        long time =0;
        try {
            time = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss")
                    .parse(dateStr)
                    .getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return time;
    }


}
