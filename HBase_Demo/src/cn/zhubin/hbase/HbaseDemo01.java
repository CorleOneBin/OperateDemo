package cn.zhubin.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HbaseDemo01 {
	
	HBaseAdmin hbaseAdmin;
	String TN = "phone";
	
	@Before
	public void begin() throws Exception {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "node1,node2,node3");
		hbaseAdmin = new HBaseAdmin(conf);
	}
	
	@After
	public void end() {
		if(hbaseAdmin != null) {
			try {
				hbaseAdmin.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Test
	public void createTable() throws IOException {
		
		if(hbaseAdmin.tableExists(TN)) {
			hbaseAdmin.disableTable(TN);
			hbaseAdmin.deleteTable(TN);
		}
		
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TN));
		HColumnDescriptor family = new HColumnDescriptor("cf1");
		family.setBlockCacheEnabled(true);
		family.setInMemory(true);
		family.setMaxVersions(1);
		desc.addFamily(family);
		hbaseAdmin.createTable(desc);
	}
	
}
