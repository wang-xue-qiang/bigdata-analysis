package com.pusidun.flink.streambatch.userinfo;


import com.pusidun.utils.HBaseUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * 1.关系型数据导出到 HDFS
 * 2.使用FLINK进行批处理
 */
public class UserInfoTask {
    public static void main(String[] args) throws Exception {
        //1.执行环境加载
        ParameterTool params = ParameterTool.fromArgs(args);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.读取文件
        DataSet<String> dataSource = env.readTextFile("./userInfo.txt");
        //2.1 map操作
        MapOperator<String, UserInfoEntity> map =
                dataSource.map(new MapFunction<String, UserInfoEntity>() {
            @Override
            public UserInfoEntity map(String s) throws Exception {
                String[] dataArray = s.split("\t");
                UserInfoEntity userInfo = new UserInfoEntity();
                userInfo.setUid(dataArray[0]);
                userInfo.setName(dataArray[1]);
                userInfo.setAge(dataArray[2]);
                userInfo.setPhone(dataArray[3]);
                userInfo.setEmail(dataArray[4]);
                userInfo.setAddress(dataArray[5]);
                //将数据写入HBase
                HBaseUtils.putData("shop:user",userInfo.getUid(),"info","name",userInfo.getName());
                HBaseUtils.putData("shop:user",userInfo.getUid(),"info","age",userInfo.getAge());
                HBaseUtils.putData("shop:user",userInfo.getUid(),"info","phone",userInfo.getPhone());
                HBaseUtils.putData("shop:user",userInfo.getUid(),"info","email",userInfo.getEmail());
                HBaseUtils.putData("shop:user",userInfo.getUid(),"info","address",userInfo.getAddress());
                return userInfo;
            }
        });
        //3.执行
        map.collect();
        env.execute();
    }




}
