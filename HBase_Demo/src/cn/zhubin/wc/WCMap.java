package cn.zhubin.wc;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMap extends Mapper<LongWritable, Text, Text, IntWritable>{

	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String str = value.toString();
		String[] strs = StringUtils.split(str,' ');
		for(String s : strs) {
			context.write(new Text(s), new IntWritable(1));
		}
	}
	
}
