
package com.pusidun.hbase.weibo;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 1.发布微博
 * 2.关注
 * 3.取消关注
 * 4.初始化信息
 * 5.获取某个人所有微博
 */
public class WeiBoDao {

    /**
     * 1.发布微博
     * @param uid 用户标识
     * @param content 发布内容
     */
    public static void publishWeiBo(String uid, String content) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        //第一部分：操作微博内容表
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        long ts = System.currentTimeMillis();
        String rowKey = uid  + "_" + ts;
        Put contentPut = new Put(Bytes.toBytes(rowKey));
        contentPut.addColumn(Bytes.toBytes(Constants.CONTENT_TABLE_CF),Bytes.toBytes("content"),Bytes.toBytes(content));
        contentTable.put(contentPut);
        //第二部分：操作微博收件箱表
        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes(Constants.RELATION_TABLE_CF2));
        Result result = relationTable.get(get);
        List<Put> inboxPuts = new ArrayList<Put>();
        for (Cell cell : result.rawCells()) {
            Put inboxPut = new Put(CellUtil.cloneQualifier(cell));
            inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF),Bytes.toBytes(uid),Bytes.toBytes(rowKey));
            inboxPuts.add(inboxPut);
        }
        //是否有粉丝
        if(inboxPuts.size()>0){
            Table inBoxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
            inBoxTable.put(inboxPuts);
            inBoxTable.close();
        }
        relationTable.close();
        contentTable.close();
        connection.close();
    }

    /**
     * 2.关注用户
     * @param uid 用户标识
     * @param attends 关注用户
     */
    public static void addAttends(String uid, String... attends) throws IOException {
        if(attends.length <= 0 ){
            System.out.println("关注信息不能为空!");
            return;
        }
        //第一步：操作用户关系表
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
        ArrayList<Put> relationPuts = new ArrayList<Put>();
        Put uidPut = new Put(Bytes.toBytes(uid));
        for (String attend : attends) {
            //给操作者对象赋值
            uidPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF),Bytes.toBytes(attend),Bytes.toBytes(attend));
            Put attendPut = new Put(Bytes.toBytes(attend));
            attendPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF2),Bytes.toBytes(uid),Bytes.toBytes(uid));
            relationPuts.add(attendPut);
        }
        relationPuts.add(uidPut);
        relationTable.put(relationPuts);
        //第二步：收件箱表
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        Put inboxPut = new Put(Bytes.toBytes(uid));
        for (String attend : attends) {
            Scan scan = new Scan(Bytes.toBytes(attend + "_"), Bytes.toBytes(attend + "|"));
            ResultScanner scanner = contentTable.getScanner(scan);
            long ts = System.currentTimeMillis();
            for (Result result : scanner) {
                //此处比较坑
                inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF),Bytes.toBytes(attend),ts++,result.getRow());
            }
        }

        if(!inboxPut.isEmpty()){
            Table inBoxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
            inBoxTable.put(inboxPut);
            inBoxTable.close();
        }
        relationTable.close();
        contentTable.close();
        connection.close();
    }

    /**
     * 3.取消关注用户
     * @param uid 用户标识
     * @param dels 关注用户
     */
    public static void deleteAttends(String uid, String... dels) throws IOException{
        if(dels.length <= 0 ){
            System.out.println("取消关信息不能为空!");
            return;
        }
        //第一部分：关系表
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
        Delete uidDelete = new Delete(Bytes.toBytes(uid));
        ArrayList<Delete> relationDeletes = new ArrayList<Delete>();
        for (String del : dels) {
            uidDelete.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF),Bytes.toBytes(del));
            Delete relationDelete = new Delete(Bytes.toBytes(del));
            relationDelete.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF2),Bytes.toBytes(uid));
            relationDeletes.add(relationDelete);
        }
        relationDeletes.add(uidDelete);
        relationTable.delete(relationDeletes);

        //第二部分：操作收件箱表
        Table  inBoxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
        Delete inBoxDelete = new Delete(Bytes.toBytes(uid));
        for (String del : dels) {
            inBoxDelete.addColumns(Bytes.toBytes(Constants.INBOX_TABLE_CF),Bytes.toBytes(del));
        }
        inBoxTable.delete(inBoxDelete);
        inBoxTable.close();
        relationTable.close();
        connection.close();
    }

    /**
     * 4.初始化信息
     * @param uid 用户标识
     */
    public static void init(String uid) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Table  inBoxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        Get inboxGet = new Get(Bytes.toBytes(uid));
        inboxGet.setMaxVersions();
        Result result = inBoxTable.get(inboxGet);
        for (Cell cell : result.rawCells()) {
            Get contGet = new Get(CellUtil.cloneValue(cell));
            Result contResult = contentTable.get(contGet);
            for (Cell contCell : contResult.rawCells()) {
                System.out.println(
                        "  RW:"+ Bytes.toString(CellUtil.cloneRow(contCell))+
                                ", CF:"+ Bytes.toString(CellUtil.cloneFamily(contCell))+
                                ", CN:"+ Bytes.toString(CellUtil.cloneQualifier(contCell))+
                                ", VALUE:"+ Bytes.toString(CellUtil.cloneValue(contCell)));
            }
        }
        contentTable.close();
        inBoxTable.close();
        connection.close();
    }

    /**
     * 5.获取所有微博
     * @param uid 用户标识
     */
    public static void getWeiBo(String uid) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        Scan scan = new Scan();
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(uid+"_"));
        scan.setFilter(rowFilter);
        ResultScanner results = contentTable.getScanner(scan);
        for (Result result : results) {
            for (Cell contCell : result.rawCells()) {
                System.out.println(
                        "  RW:"+ Bytes.toString(CellUtil.cloneRow(contCell))+
                                ", CF:"+ Bytes.toString(CellUtil.cloneFamily(contCell))+
                                ", CN:"+ Bytes.toString(CellUtil.cloneQualifier(contCell))+
                                ", VALUE:"+ Bytes.toString(CellUtil.cloneValue(contCell)));
            }
        }
        contentTable.close();
        connection.close();
    }


}
