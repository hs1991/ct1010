package com.sh.conprocessor;

import com.sh.constant.Constant;
import com.sh.utils.HBaseUtil;
import com.sh.utils.PropertityUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class MyConprocessor extends BaseRegionObserver {

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {

        //获取协处理器中的表
        String newTable = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
        //获取当前操作的表
        String oldTable = PropertityUtil.getPropertity().getProperty("hbase.table.name");

        if (!newTable.equals(oldTable)) {
            return;
        }

        String rowKey = Bytes.toString(put.getRow());
        String[] split = rowKey.split("_");

        if ("0".equals(split[5])) {
            return;
        }

        //获取所有字段
        String caller = split[1];
        String buildTime = split[2];
        String buildTS = split[3];
        String callee = split[4];
        String duration = split[6];

        //获取region数
        int regions = Integer.valueOf(PropertityUtil.getPropertity().getProperty("hbase.regions"));

        //生成新的分区号
        String rowHash = HBaseUtil.getRowHash(regions, callee, buildTime);

        //生成被叫rowkey
        String newRowKey = HBaseUtil.getRowKey(rowHash, callee, buildTime, buildTS, caller, "0", duration);

        //添加数据
        Put newPut = new Put(Bytes.toBytes(newRowKey));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call1"), Bytes.toBytes(callee));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("buildTime"), Bytes.toBytes(buildTime));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("buildTS"), Bytes.toBytes(buildTS + ""));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call2"), Bytes.toBytes(caller));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("flag"), Bytes.toBytes("0"));
        newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("duration"), Bytes.toBytes(duration));

        //获取HBase连接以及表对象
        Connection connection = ConnectionFactory.createConnection(Constant.CONF);
        Table table = connection.getTable(TableName.valueOf(oldTable));

        //插入被叫数据
        table.put(newPut);
        //关闭资源
        table.close();
        connection.close();
    }
}
