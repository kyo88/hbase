package hbase.client;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;

import java.util.Date;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class HbaseBatch {
	
	private final static byte[] ROW1 = Bytes.toBytes("row1");
	private final static byte[] COLFAM1 = Bytes.toBytes("col1");
	private final static byte[] QUAL1 = Bytes.toBytes("qual1");
	
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
      
        List<Row> batch = new ArrayList<Row>();

        Put put = new Put(ROW1);
        put.add(COLFAM1, QUAL1, Bytes.toBytes("val2"));
        batch.add(put);

        Get get1 = new Get(ROW1);
        get1.addColumn(COLFAM1, QUAL1);
        batch.add(get1);

//        Delete delete = new Delete(ROW1);
//        delete.deleteColumns(COLFAM1, QUAL1, 3L); // co BatchSameRowExample-1-AddDelete Delete the row that was just put above.
//        batch.add(delete);

        Get get2 = new Get(Bytes.toBytes("col1"));
//        get1.addColumn(COLFAM1, QUAL1);
        batch.add(get2);

        Object[] results = new Object[batch.size()];
        try {
          System.out.println("After batch call...1");
          table.batch(batch, results);
          System.out.println("After batch call...2");
        } catch (Exception e) {
          System.err.println("Error: " + e);
        }
        // ^^ BatchSameRowExample

        for (int i = 0; i < results.length; i++) {
          System.out.println("Result[" + i + "]: " + results[i]);
        }
        System.out.println("After batch call...");
        //helper.dump("testtable", new String[]{ "row1" }, null, null);
    }

}