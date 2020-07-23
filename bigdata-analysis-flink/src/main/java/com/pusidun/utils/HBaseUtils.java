package com.pusidun.utils;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * DML和 DDL
 **/
public class HBaseUtils {

    /**初始化*/
    public static Connection connection = null;
    public static Admin admin = null;
    static {
        try {
            //1.配置参数
            Configuration conf = HBaseConfiguration.create();
            //conf.set("hbase.zookeeper.quorum", "bigdata-node01.com,bigdata-node02.com,bigdata-node03.com");
            //conf.set("hbase.zookeeper.quorum", "node1.com,node2.com,node3.com");
            conf.set("hbase.zookeeper.quorum", "bigdata-cloudera-master.com");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            //2.创建连接对象
            connection = ConnectionFactory.createConnection(conf);
            //3.Admin
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断表是否存在
     * @param tableName 表名
     * @return 返回boolean
     */
    public static boolean isTableExist(String tableName) {
        boolean exist = false;
        try {
            exist = admin.tableExists(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return exist;
    }

    /**
     * 创建表
     * @param tableName 表名
     * @param cfs 列族
     */
    public static void createTable(String tableName, String... cfs){
        //1.判断列族是否存在
        if(cfs.length <= 0){
            System.out.println("请设置列族信息!");
            return;
        }
        //2.判断表是否存在
        if(isTableExist(tableName)){
            System.out.println(tableName +"表已存在!");
            return;
        }
        //3.创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //4.循环添加列族信息
        for (String cf : cfs) {
            //5.创建列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        //6.创建表
        try {
            admin.createTable(hTableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除表
     * @param tableName 表名
     */
    public static void dropTable(String tableName){
        if(!isTableExist(tableName)){
            System.out.println(tableName +"表不存在!");
            return;
        }
        try {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 创建命名空间
     * @param ns 命名空间
     */
    public static void createNameSpace(String ns){
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();
        try {
            admin.createNamespace(namespaceDescriptor);
        }catch (NamespaceExistException e){
            System.out.println(ns +" 命名空间已存在！");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭资源
     */
    public static void close(){
        if(null != admin){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(null != connection){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 插入数据
     * @param tableName 表名
     * @param rowKey 主键
     * @param cf 列簇
     * @param cn 列名
     * @param val 值
     */
    public static void putData(String tableName,String rowKey,String cf ,String cn,String val){
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(val));
            table.put(put);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取数据
     * @param tableName 表名
     * @param rowKey 主键
     * @param cf 列簇
     * @param cn 列名
     */
    public static String getData(String tableName,String rowKey,String cf ,String cn){
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
            Result result = table.get(get);
/*
            for (Cell cell : result.rawCells()) {
                System.out.println("CF:"+ Bytes.toString(CellUtil.cloneFamily(cell))+
                        ",CN:"+ Bytes.toString(CellUtil.cloneQualifier(cell))+
                        ",VALUE:"+ Bytes.toString(CellUtil.cloneValue(cell)));
            }
*/
            byte[] resultBytes = result.getValue(cf.getBytes(), cn.getBytes());
            table.close();
            if (resultBytes == null) {
                return null;
            }
            return new String(resultBytes);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return  null;
    }

    /**
     * 获取数据
     * @param tableName 表名
     * @param rowKey 主键
     */
    public static void getRowKeyData(String tableName,String rowKey){
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);

            for (Cell cell : result.rawCells()) {
                System.out.println(
                        "列族:"+ Bytes.toString(CellUtil.cloneFamily(cell))+
                        "\t列名:"+ Bytes.toString(CellUtil.cloneQualifier(cell))+
                        "\t值:"+ Bytes.toString(CellUtil.cloneValue(cell)));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 扫描表
     * @param tableName 表名称
     */
    public static void scanTable(String tableName){
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            ResultScanner results = table.getScanner(scan);
            for (Result result : results) {
                for (Cell cell : result.rawCells()) {
                    System.out.println(
                            "列族:"+ Bytes.toString(CellUtil.cloneFamily(cell))+
                            "\t列名:"+ Bytes.toString(CellUtil.cloneQualifier(cell))+
                            "\t值:"+ Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }

            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除数据 scan 'student',{RAW=>TRUE,VERSIONS=>5}
     * @param tableName 表名
     * @param rowKey 主键
     * @param cf 列簇
     * @param cn 列名
     */
    public static void deleteData(String tableName,String rowKey,String cf ,String cn){
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            //删除多个版本
            delete.addColumns(Bytes.toBytes(cf),Bytes.toBytes(cn));
            //删除单个 生产禁止
            //delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
            table.delete(delete);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        //createNameSpace("shop");
        //createTable("shop:user","info");
        getRowKeyData("shop:user","0000000003");
    }

}
