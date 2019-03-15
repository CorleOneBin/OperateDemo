package cn.zhubin.hbase;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HbaseDemo {
	
	HBaseAdmin hBaseAdmin;
	HTable hTable;
	Random r = new Random();
	
	String TN = "phone";
	
	
	@Before
	public void begin() throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "node1,node2,node3");
		hBaseAdmin = new HBaseAdmin(conf);
		hTable = new HTable(conf, TN);
	}
	
	@After
	public void end() {
		if(hBaseAdmin != null) {
			try {
				hBaseAdmin.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if(hTable != null) {
			try {
				hTable.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * ������
	 * @throws IOException 
	 */
	@Test
	public void createTbl() throws IOException {
		
		if(hBaseAdmin.tableExists(TN)) {
			hBaseAdmin.disableTable(TN);
			hBaseAdmin.deleteTable(TN);
		}
		
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TN));       //
		HColumnDescriptor family = new HColumnDescriptor("cf1");
		
		family.setBlockCacheEnabled(true);     //�������������
		family.setInMemory(true);
		family.setMaxVersions(1);
		
		desc.addFamily(family);                //�������
		
		hBaseAdmin.createTable(desc);
		
	}
	
	/**
	 * ��������
	 * @throws IOException
	 */
	@Test
	public void insert() throws IOException {
		
		//�ֻ���+ʱ�� ��Ϊrowkey
		String rowkey = "15274464875_20180706";
		Put put = new Put(rowkey.getBytes());
		
		put.addColumn("cf1".getBytes(), "name".getBytes(), "zhangsan".getBytes());
		put.addColumn("cf1".getBytes(), "type".getBytes(), "0".getBytes());
		put.addColumn("cf1".getBytes(), "pnum".getBytes(), "16473382736".getBytes());
		
		hTable.put(put);
	}
	
	/**
	 *��ȡ���� 
	 * @throws Exception
	 */
	@Test
	public void get() throws Exception {
		
		//�ֻ���+ʱ�� ��Ϊrowkey
		String rowkey = "15274464875_20180706";
		Get get = new Get(rowkey.getBytes());
		get.addColumn("cf1".getBytes(), "name".getBytes());
		get.addColumn("cf1".getBytes(), "type".getBytes());
		Result rs = hTable.get(get);
		Cell cell = rs.getColumnLatestCell("cf1".getBytes(), "name".getBytes());
		System.out.println(new String(CellUtil.cloneValue(cell)));
	}
	
	/**
	 * �����õ绰����
	 * @param prefix
	 * @return
	 */
	public String getPhoneNum(String prefix) {
		return prefix + String.format("%08d", r.nextInt(99999999));
	}
	
	/**
	 * �������ʱ��    ָ����
	 */
	public String getDate(String year) {
		return year + String.format("%02d%02d%02d%02d%02d",
									new Object[]{r.nextInt(12)+1,r.nextInt(29)+1,
									r.nextInt(24),r.nextInt(60),r.nextInt(60)});
	}
	
	/**
	 * �������ʱ��   ָ��������   �������ʱ����
	 * @param prefix
	 * @return
	 */
	public String getDate2(String prefix) {
		return prefix + String.format("%02d%02d%02d", new Object[] {
				r.nextInt(24),r.nextInt(60),r.nextInt(60)
		});
	}
	
	/**
	 * ����ʮ���ֻ���100��ͨ����¼
	 * �����ѯ  ��ʱ�併������
	 * @throws Exception 
	 */
	@Test
	public void insertDB() throws Exception {
		List<Put> puts = new ArrayList<>();
		for(int i = 0 ; i < 10 ; i++) {
			String rowkey;
			String phoneNum = getPhoneNum("186");
			for(int j = 0; j < 100; j++) {
				String phoneDate = getDate("2016");
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
				try {
					long date = Long.MAX_VALUE - sdf.parse(phoneDate).getTime();    //��һ�����ֵ��ȥʱ�䣬��ʵ�ֽ�������
					System.out.println(sdf.parse(phoneDate).getTime());
					rowkey = phoneNum + date;
					
					Put put = new Put(rowkey.getBytes());
					
					put.addColumn("cf1".getBytes(), "type".getBytes(), (r.nextInt(2)+"").getBytes() );
					put.addColumn("cf1".getBytes(), "time".getBytes(), (phoneDate).getBytes() );
					put.addColumn("cf1".getBytes(), "pnum".getBytes(), (getPhoneNum("152")).getBytes() );
					 
					puts.add(put);
					
				} catch (ParseException e) {
					e.printStackTrace();
				} 
			}
		}
		hTable.put(puts);
	}
	
	/**
	 * ��ʮ���ֻ��ţ�ÿ���ֻ���һ���ͨ����¼��Ϊһ����¼�����hbase��  
	 * @throws Exception 
	 */
	@Test
	public void insertDB2() throws Exception {
		for(int i = 0 ; i < 10; i++) {
			String phoneNum = getPhoneNum("152");
			String date = ""+(Long.MAX_VALUE - Long.parseLong("20180724"));
			String rowkey = phoneNum +"_"+date ;
			Phone.pday.Builder pday = Phone.pday.newBuilder();
			for(int j = 0 ; j < 100; j++) {
				String pnum = getPhoneNum("188");                //�Է��绰
				String time = getDate2("20180724");
				String type = String.valueOf(r.nextInt(2));
				Phone.pdetail.Builder pdetail = Phone.pdetail.newBuilder();
				pdetail.setPnum(pnum);
				pdetail.setTime(time);
				pdetail.setType(type);
				
				pday.addPlist(pdetail);
				
 			}
			Put put = new Put(rowkey.getBytes());
			put.addColumn("cf1".getBytes(), "pday".getBytes(), pday.build().toByteArray());
			hTable.put(put);
		}
	}
	
	/**
	 * �õ� 15292935715�ֻ���   ��һ�������ͨ����¼
	 * rowkey �� 15292935715_9223372036834595083
	 * @throws Exception 
	 */
	@Test
	public void getPhoneData() throws Exception {
		String rowkey = "15292935715_9223372036834595083";
		Get get = new Get(rowkey.getBytes());
		get.addColumn("cf1".getBytes(), "pday".getBytes());
		Result rs = hTable.get(get);
		Cell cell = rs.getColumnLatestCell("cf1".getBytes(), "pday".getBytes());
		Phone.pday pday = Phone.pday.parseFrom(CellUtil.cloneValue(cell));
		for(Phone.pdetail pdetail : pday.getPlistList()) {
			System.out.println(pdetail.getPnum() + "-" + pdetail.getTime() + "-" + pdetail.getType());
		}
	}
	
	/**
	 * ���һ���ֻ����룬ĳ��ʱ���ڵ�ͨ����¼
	 * @throws Exception
	 */
	@Test
	public void scanDB() throws Exception {
		
		Scan scan = new Scan();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		String startRow = "18685247648"+(Long.MAX_VALUE - sdf.parse("20160401000000").getTime());
		String stopRow = "18685247648"+(Long.MAX_VALUE - sdf.parse("20160301000000").getTime());
		scan.setStartRow(startRow.getBytes());
		scan.setStopRow(stopRow.getBytes());
		
		ResultScanner rss = hTable.getScanner(scan);
		for(Result rs : rss) {
			System.out.println(new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf1".getBytes(), "type".getBytes())))
					+"-"+new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf1".getBytes(), "pnum".getBytes())))
					+"-"+new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf1".getBytes(), "time".getBytes()))));
		}
	}
	
	/**
	 * ʹ�ù���������ĳ���ֻ����룬������������Ϊ0���ֻ�����
	 * @throws Exception 
	 */
	@Test
	public void scanDB2() throws Exception {
		FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		PrefixFilter pre = new PrefixFilter("18685247648".getBytes());              //��ǰ����rowkey  Ҫ������绰����ƥ��
		list.addFilter(pre);
		
		SingleColumnValueExcludeFilter singleColunmValueExcludeFilter = new SingleColumnValueExcludeFilter(
				"cf1".getBytes(), "type".getBytes(), CompareOp.EQUAL, "0".getBytes());   //��0 ��ȵ�   equal
		list.addFilter(singleColunmValueExcludeFilter);
		
		Scan scan = new Scan();
		scan.setFilter(list);
		
		ResultScanner rss = hTable.getScanner(scan);
		for(Result rs : rss) {
			String rowkey = new String(rs.getColumnLatest("cf1".getBytes(), "pnum".getBytes()).getRow());
			System.out.print(rowkey+"-");
			System.out.println(new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf1".getBytes(), "type".getBytes())))
					+"-"+new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf1".getBytes(), "pnum".getBytes())))
					+"-"+new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf1".getBytes(), "time".getBytes()))));
		}
	}
	
}
