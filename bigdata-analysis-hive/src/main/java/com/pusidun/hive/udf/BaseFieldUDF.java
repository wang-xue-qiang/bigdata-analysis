package com.pusidun.hive.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * add jar /opt/jars/my-hive-udf.jar
 *
 * create temporary function base_analizer as 'com.pusidun.hive.udf.BaseFieldUDF';
 *
 * create temporary function flat_analizer as 'com.pusidun.hive.udtf.EventJsonUDTF';
 */
public class BaseFieldUDF extends UDF {

    public String evaluate(String line, String jsonkeysString){

        // 0
        StringBuilder sb = new StringBuilder();

        // 1 jsonkeysString 切割
        String[] jsonkeys = jsonkeysString.split(",");

        // 2 line  服务器时间| json
        if (line== null){
            return "";
        }
        String[] logContents = line.split("\\|");

        if (logContents.length != 2 || StringUtils.isBlank(logContents[1])){
            return "";
        }

        // 3 创建json对象
        try {
            JSONObject jsonObject = new JSONObject(logContents[1]);

            JSONObject cm = jsonObject.getJSONObject("cm");

            for (int i = 0; i < jsonkeys.length; i++) {
                String jsonkey = jsonkeys[i];

                if(cm.has(jsonkey)){
                    sb.append(cm.getString(jsonkey)).append("\t");
                }else {
                    sb.append("\t");
                }
            }

            sb.append(jsonObject.getString("et")).append("\t");
            sb.append(logContents[0]).append("\t");

        } catch (JSONException e) {
            e.printStackTrace();
        }

        return sb.toString();
    }

    public static void main(String[] args) {
        //String line = "1541217850324|{\"cm\":{\"mid\":\"m7856\",\"uid\":\"u8739\",\"ln\":\"-74.8\",\"sv\":\"V2.2.2\",\"os\":\"8.1.3\",\"g\":\"P7XC9126@gmail.com\",\"nw\":\"3G\",\"l\":\"es\",\"vc\":\"6\",\"hw\":\"640*960\",\"ar\":\"MX\",\"t\":\"1541204134250\",\"la\":\"-31.7\",\"md\":\"huawei-17\",\"vn\":\"1.1.2\",\"sr\":\"O\",\"ba\":\"Huawei\"},\"ap\":\"weather\",\"et\":[{\"ett\":\"1541146624055\",\"en\":\"display\",\"kv\":{\"goodsid\":\"n4195\",\"copyright\":\"ESPN\",\"content_provider\":\"CNN\",\"extend2\":\"5\",\"action\":\"2\",\"extend1\":\"2\",\"place\":\"3\",\"showtype\":\"2\",\"category\":\"72\",\"newstype\":\"5\"}},{\"ett\":\"1541213331817\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"15\",\"action\":\"3\",\"extend1\":\"\",\"type1\":\"\",\"type\":\"3\",\"loading_way\":\"1\"}},{\"ett\":\"1541126195645\",\"en\":\"ad\",\"kv\":{\"entry\":\"3\",\"show_style\":\"0\",\"action\":\"2\",\"detail\":\"325\",\"source\":\"4\",\"behavior\":\"2\",\"content\":\"1\",\"newstype\":\"5\"}},{\"ett\":\"1541202678812\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1541184614380\",\"action\":\"3\",\"type\":\"4\",\"content\":\"\"}},{\"ett\":\"1541194686688\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"3\"}}]}";
        String line = "1603263026270|{\"cm\":{\"ln\":\"-69.6\",\"sv\":\"V2.4.9\",\"os\":\"8.2.1\",\"g\":\"X85SGS4O@gmail.com\",\"groupId\":\"58\",\"mid\":\"0\",\"nw\":\"4G\",\"l\":\"en\",\"vc\":\"16\",\"hw\":\"1080*1920\",\"ar\":\"US\",\"uid\":\"0\",\"t\":\"1603195364496\",\"la\":\"-35.0\",\"md\":\"HTC-3\",\"vn\":\"1.0.5\",\"ba\":\"HTC\",\"sr\":\"Z\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1603233216630\",\"en\":\"clickRank\",\"kv\":{\"action\":\"1\"}},{\"ett\":\"1603182368545\",\"en\":\"shopWindow\",\"kv\":{\"action\":\"6\",\"buyKind\":\"9\"}},{\"ett\":\"1603181642975\",\"en\":\"gift\",\"kv\":{\"level\":2137,\"action\":\"ad\"}},{\"ett\":\"1603177659129\",\"en\":\"background\",\"kv\":{\"active_source\":\"3\"}}]}\t2020-10-21\n";
        String x = new BaseFieldUDF().evaluate(line, "mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t,groupId");
        System.out.println(x);
    }

}
