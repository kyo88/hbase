package hbase.client;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import java.util.Date;


public class HbaseTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        //TestRegionSplitter test = new TestRegionSplitter();
        //test.testCreatePresplitTableHex();
        HBaseAdmin admin = new HBaseAdmin(conf);
        HConnection connection = HConnectionManager.createConnection(conf);
        HTableInterface table = connection.getTable("myTable");
        try {
            //HTable table = new HTable(conf, "hbase_test");
            Long index=new Long(0);
            while(true){
            Long time = Long.MAX_VALUE - (Long)System.currentTimeMillis();
            java.util.Date date= new java.util.Date();
            System.out.println("AAAAAA - "+ time);
            Put put = new Put(Bytes.toBytes(time.toString()));
            index++;
            put.add(Bytes.toBytes("f1"),Bytes.toBytes("long"), Bytes.toBytes(index.toString()));
            table.put(put);
            
            table.get(new Get(Bytes.toBytes("aaaaaaaaaa")));
            //Thread.sleep(2000);
            }
        } finally {
            admin.close();
        }
    }

}
