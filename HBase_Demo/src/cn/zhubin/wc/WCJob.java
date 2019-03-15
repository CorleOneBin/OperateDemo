package cn.zhubin.wc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class WCJob {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		//conf.set("fs.defaultFS","hdfs://node1:8020");       //����srcĿ¼�µ������ļ����Լ����ã���win����������
	//	conf.set("yarn.resourcemanager.hostname","node3");  
		conf.set("hbase.zookeeper.quorum", "node1,node2,node3");
		conf.set("mapred.jar","D://mac.jar");
		try {
			Job job = Job.getInstance(conf);
			job.setJarByClass(WCJob.class);
			job.setMapperClass(WCMap.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			//Reducer
			
			FileInputFormat.addInputPath(job, new Path("/wc/input"));
			
			String tableName = "wc";
			TableMapReduceUtil.initTableReducerJob(
					tableName,            //ָ������ı��� 
					WCReducer.class, 
					job);
			
			boolean flag = job.waitForCompletion(true);
			if(flag) {
				System.out.println("Job success");
			}
		} catch (IllegalStateException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
