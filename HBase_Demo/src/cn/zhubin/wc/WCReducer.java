package cn.zhubin.wc;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class WCReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable>{

	public static final byte[] CF = "cf".getBytes();
	public static final byte[] COUNT = "count".getBytes();
	
	protected void reduce(Text key, Iterable<IntWritable> value,
			Context context)
			throws IOException, InterruptedException {
		int i = 0;
		for(IntWritable val : value) {
			i+=val.get();
		}
		Put put = new Put(key.toString().getBytes());
		put.add(CF, COUNT, String.valueOf(i).getBytes());
		context.write(null, put);
	}
	
}
