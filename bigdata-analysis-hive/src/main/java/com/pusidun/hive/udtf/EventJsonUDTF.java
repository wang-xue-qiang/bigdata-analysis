package com.pusidun.hive.udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;

public class EventJsonUDTF extends GenericUDTF{

    @Deprecated
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {

        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldTypes = new ArrayList<>();

        fieldNames.add("event_name");
        fieldTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        fieldNames.add("event_json");
        fieldTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldTypes);
    }

    @Override
    public void process(Object[] objects) throws HiveException {

        // 1 获取数据
        String input = objects[0].toString();

        // 2 校验
        if (StringUtils.isBlank(input)){
            return;
        }else {
            try {
                JSONArray jsonArray = new JSONArray(input);
                
                if (jsonArray == null){
                    return;
                }

                for (int i = 0; i < jsonArray.length(); i++) {

                    String[] results = new String[2];

                    try {
                        // 获取事件名称
                        results[0]= jsonArray.getJSONObject(i).getString("en");

                        results[1] =jsonArray.getString(i);
                    } catch (JSONException e) {
                        e.printStackTrace();
                        continue;
                    }

                    // 把结果写出去
                    forward(results);
                }

            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public void close() throws HiveException {

    }
}
