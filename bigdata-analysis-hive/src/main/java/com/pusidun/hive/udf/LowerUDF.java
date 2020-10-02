package com.pusidun.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 转小写
 *
 * hive> add jar /opt/data/lower-udf.jar;
 * hive> create temporary function mylower as "com.pusidun.hadoop.hive.udf.LowerUDF";
 * hive> select ename, mylower(ename) lowername from emp;
 */
public class LowerUDF extends UDF {

    public String evaluate (String s) {
        if (s == null) {
            return null;
        }
        return s.toLowerCase();
    }

}
