package com.sh.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.DecimalFormat;

/**
 * 1.创建命名空间
 * 2.判断表是否存在
 * 3.创建表
 * 4.生成rowkey
 * 5.预分区健的生成
 */


public class HBaseUtil {
    private static Configuration conf;

    static {
        conf = HBaseConfiguration.create();
    }

    //1.创建命名空间
    public static void createNamespace(String nameSpace) throws Exception {

        //获取连接对象
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        //获取命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        //创建命名空间
        admin.createNamespace(namespaceDescriptor);
        //关闭资源
        admin.close();
        connection.close();
    }
    //2.判断表是否存在
    public static boolean existTable(String tableName) throws Exception {
        //获取连接对象
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        //判断
        boolean tableExists = admin.tableExists(TableName.valueOf(tableName));
        //关闭资源
        admin.close();
        connection.close();

        return tableExists;
    }
    //3.创建表
    public static void createTable(String tableName, int regions,String... cfs) throws Exception {
        //获取连接对象
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        //判断表是否存在
        if (existTable(tableName)) {
            System.out.println("表：" + tableName + "已存在！");
            //关闭资源
            admin.close();
            connection.close();
            return;
        }
        //创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //循环添加列族
        for (String cf : cfs) {
            //创建列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        //创建表
        admin.createTable(hTableDescriptor,getSplitKeys(regions));
        //关闭资源
        admin.close();
        connection.close();
    }
    //预分区健的生成
    //00|,01|,02|,03|,04|,05|
    public static byte[][] getSplitKeys(int regions) {

        //创建分区健二维数据
        byte[][] splitKeys = new byte[regions][];
        DecimalFormat df = new DecimalFormat("00");

        //循环添加分区健
        for (int i = 0; i < regions; i++) {
            splitKeys[i] = Bytes.toBytes(df.format(i) + "|");
        }

        return splitKeys;
    }
    //生成rowkey
    //0x_13712341234_2017-05-02 12:23:55_时间戳_13598769876_duration
    public static String getRowKey(String rowHash, String caller, String buildTime, String buildTS, String callee, String flag,String duration) {

        return rowHash + "_"
                + caller + "_"
                + buildTime + "_"
                + buildTS + "_"
                + callee + "_"
                +flag+"_"
                + duration;
    }

    //生成分区号（13712341234，2017-05-02 12:23:55）
    public static String getRowHash(int regions, String caller, String buildTime) {

        DecimalFormat df = new DecimalFormat("00");

        //取手机号中间4位
        String phoneMid = caller.substring(3, 7);
        String yearMonth = buildTime.replace("-", "").substring(0, 6);

        int i = (Integer.valueOf(phoneMid) ^ Integer.valueOf(yearMonth)) % regions;

        return df.format(i);
    }


    public static void main(String[] args) {

        byte[][] splitKeys = getSplitKeys(6);
        for (byte[] splitKey : splitKeys) {
            System.out.println(Bytes.toString(splitKey) + "--");
        }

        String rowHash = getRowHash(6, "15595505995", "2017-07-22 09:43:51");

        System.out.println(getRowKey(rowHash, "15595505995", "2017-07-22 09:43:51", "4545", "15178485516","1", "0235"));

    }
}
