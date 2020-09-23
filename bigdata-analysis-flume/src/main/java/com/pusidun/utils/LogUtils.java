package com.pusidun.utils;

import org.apache.commons.lang.math.NumberUtils;

/**
 * 数据简单清洗
 */
public class LogUtils {
    //校验启动日志
    public static boolean valuateStart(String log) {
        if (null == log) return false;
        if (!log.trim().startsWith("{") || !log.trim().endsWith("}")) return false;
        return true;
    }

    //校验启动事件
    public static boolean valuateEvent(String log) {
        if (null == log) return false;
        String[] logContents = log.split("\\|");
        if (logContents.length != 2) return false;
        if (logContents[0].length() != 13 || !NumberUtils.isDigits(logContents[0])) return false;
        if (!logContents[1].trim().startsWith("{") || !logContents[1].trim().endsWith("}")) return false;
        return true;
    }
}
