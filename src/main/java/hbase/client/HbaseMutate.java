package hbase.client;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;

import java.util.Date;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseMutate {
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
//        	System.out.println("  Location\tData 01");
//            Long index=new Long(0);
//            while(true){
//            Long time = Long.MAX_VALUE - (Long)System.currentTimeMillis();
//            java.util.Date date= new java.util.Date();
//            System.out.println("AAAAAA - "+ time);
//            Put put = new Put(Bytes.toBytes(time.toString()));
//            index++;
//            put.add(Bytes.toBytes("col1"),Bytes.toBytes("col1"), Bytes.toBytes(index.toString()));
//            index++;
//            put.add(Bytes.toBytes("col1"),Bytes.toBytes("col1"), Bytes.toBytes(index.toString()));
//            table.put(put);
//            
//            table.get(new Get(Bytes.toBytes("aaaaaaaaaa")));
            
//            byte[] invalid = { (byte) 'f', (byte) 'o', (byte) 'o', (byte) '-', (byte) 0xfc, (byte) 0xa1, (byte) 0xa1, (byte) 0xa1, (byte) 0xa1 };
//            byte[] valid = { (byte) 'f', (byte) 'o', (byte) 'o', (byte) '-', (byte) 0xE7, (byte) 0x94, (byte) 0x9F, (byte) 0xE3, (byte) 0x83, (byte) 0x93, (byte) 0xE3, (byte) 0x83, (byte) 0xBC, (byte) 0xE3, (byte) 0x83, (byte) 0xAB};
            Put put = new Put(Bytes.toBytes("col5"));
            put.add(Bytes.toBytes("col1"),Bytes.toBytes("col3"), Bytes.toBytes("longta11111"));
            Put put2 = new Put(Bytes.toBytes("col5"));
            put2.add(Bytes.toBytes("col1"),Bytes.toBytes("col4"), Bytes.toBytes("longta33333"));
            RowMutations rowMutations = new RowMutations(Bytes.toBytes("col5"));
            
            Get get = new Get(Bytes.toBytes("col1"));
            
            rowMutations.add(put2);
            rowMutations.add(put);
//            rowMutations.add(get);
//            Delete delete = new Delete(Bytes.toBytes("col5"));
//            delete.deleteColumns(Bytes.toBytes("col1"), Bytes.toBytes("col4"));
//            rowMutations.add(delete);
            table.mutateRow(rowMutations);
//            table.put(put2);
            
            System.out.println("  Location\tData");
            //Thread.sleep(2000);
//            break;
            
        } finally {
//        	System.out.println("  Can't create connection");
        	connection.close();
        }
    }

}
