package com.sh.consumer;

import com.sh.constant.Constant;
import com.sh.utils.HBaseUtil;
import com.sh.utils.PropertityUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 1.初始化命名空间
 * 2.创建表
 * 3.批量存储数据
 */
public class HbaseDao
{

    //配置信息
    private Properties properties;
    //命名空间
    private String nameSpace;
    //表名
    private String tableName;
    //分区数
    private int regions;
    //列族
    private String cf;
    //
    private SimpleDateFormat sdf;
    //HBase连接
    private Connection connection;
    //HBase表对象
    private Table table;
    //
    private List<Put> puts;
    public HbaseDao() throws Exception{
        //初始化相应的参数
        properties = PropertityUtil.getPropertity();
        nameSpace = properties.getProperty("hbase.namespace");
        tableName = properties.getProperty("hbase.table.name");
        regions = Integer.valueOf(properties.getProperty("hbase.regions"));
        cf = properties.getProperty("hbase.cf");
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //初始化命名空间
  //      HBaseUtil.createNamespace(nameSpace);
        //创建表
        HBaseUtil.createTable(tableName, regions, cf);
        connection = ConnectionFactory.createConnection(Constant.CONF);
        table = connection.getTable(TableName.valueOf(tableName));
        puts = new ArrayList<Put>();
    }
    public void put(String value) throws Exception {
        //判断传输过来的数据是否异常
        if (value == null) {
            return;
        }

        //value:19879419704,18302820904,2017-03-27 21:10:56,1150
        String[] split = value.split(",");

        //第一个号码
        String call1 = split[0];
        //第二个号码
        String call2 = split[1];
        //通话建立时间
        String buildTime = split[2];
        //通话时长
        String duration = split[3];
        //通话建立时间时间戳形式
        long buildTS = sdf.parse(buildTime).getTime();
        //生成分区号
        String rowHash = HBaseUtil.getRowHash(regions, call1, buildTime);
        //生成rowkey
        String rowKey = HBaseUtil.getRowKey(rowHash, call1, buildTime, buildTS + "", call2,"1",duration);
        //生成put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("call1"), Bytes.toBytes(call1));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("buildTime"), Bytes.toBytes(buildTime));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("buildTS"), Bytes.toBytes(buildTS + ""));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("call2"), Bytes.toBytes(call2));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("flag"), Bytes.toBytes("1"));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("duration"), Bytes.toBytes(duration));
       puts.add(put);
        if (puts.size() > 20) {
            table.put(puts);
            puts.clear();
        }
    }
    public void close() throws Exception {
        table.put(puts);
        table.close();
        connection.close();
    }
}
