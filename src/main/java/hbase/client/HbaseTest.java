package hbase.client;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;


import java.util.Date;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        System.out.println("  Location");
        //TestRegionSplitter test = new TestRegionSplitter();
        //test.testCreatePresplitTableHex();
//        System.out.println("  Location01");
        conf.set("hbase.master", "10.112.21.71:60000");
        conf.set("hbase.zookeeper.quorum", "vm27801072,vm27801073,vm27801074");
//        System.out.println("  Location01-1");
        //HBaseAdmin admin = new HBaseAdmin(conf);
//        System.out.println("  Location01-2");
        HConnection connection = HConnectionManager.createConnection(conf);
//        System.out.println("  Location01-3");
        HTableInterface table = connection.getTable("test");
//        System.out.println("  Location02");
        try {
            //HTable table = new HTable(conf, "hbase_test");
        	System.out.println("  Location\tData 01");
            Long index=new Long(0);
            while(true){
            Long time = Long.MAX_VALUE - (Long)System.currentTimeMillis();
            java.util.Date date= new java.util.Date();
            System.out.println("AAAAAA - "+ time);
            Put put = new Put(Bytes.toBytes("ccccccccc"));
            index++;
            put.add(Bytes.toBytes("col1"),Bytes.toBytes("col1"), Bytes.toBytes(index.toString()));
            index++;
            
            Put put2 = new Put(Bytes.toBytes("aaaaaaaaa"));
            put2.add(Bytes.toBytes("col1"),Bytes.toBytes("col1"), Bytes.toBytes(index.toString()));
            
            List<Put> list = new ArrayList<Put>();
            list.add(put);
            list.add(put2);
            table.put(list);
            
//            table.get(new Get(Bytes.toBytes("bbbbbbbbbbbbbb")));
            
            System.out.println("  Location\tData");
            //Thread.sleep(2000);
            break;
            }
        } finally {
        	System.out.println("  Can't create connection");
        	connection.close();
        }
    }

}
