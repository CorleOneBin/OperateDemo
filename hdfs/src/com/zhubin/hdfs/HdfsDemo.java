package com.zhubin.hdfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class HdfsDemo {
	
	FileSystem fs;
	Configuration conf;
	
	@Before
	public void begin() throws IOException {
		//默认在src目录下
		conf = new Configuration(); 
		
		fs = FileSystem.get(conf);
	}
	
	@After
	public void end() {
		try {
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void mkdir() throws Exception {
	    Path path = new Path("/tmp1");
	    fs.mkdirs(path);
	}
	
	@Test
	public void upload() throws Exception {
		Path path = new Path("/tmp/test");
		FSDataOutputStream outputstream =  fs.create(path);
		FileUtils.copyFile(new File("D://test.txt"), outputstream);      //上传文件，不是写入
	}
	


	
	@Test
	public void list() throws Exception {
		Path path = new Path("/tmp");
		FileStatus[] fss = fs.listStatus(path);
		for(FileStatus s:fss) {
			System.out.println(s.getPath()+"---"+s.getLen()+"---"+s.getAccessTime());
		}
	}
	
	//上传小文件
	@Test
	public void upload2() throws Exception {
		Path path = new Path("/tmp/seq");
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, Text.class, Text.class);
		File file = new File("D://test");
		for(File f : file.listFiles()) {
			writer.append(new Text(f.getName()),new Text(FileUtils.readFileToString(f)));
		}
	}
	
	
	//下载小文件
	@Test
	public void download2() throws Exception {
		Path path = new Path("/tmp/seq");
		SequenceFile.Reader reader = new SequenceFile.Reader(fs,path,conf);
		Text key = new Text();
		Text value = new Text();
		while(reader.next(key, value)) {
			File file = new File("D://test//bak"+key);   //创建文件下载路路径
			if(!file.exists()) {
				file.createNewFile();
			}
			FileOutputStream fos = new FileOutputStream(file);
			fos.write(value.getBytes());
			System.out.println(value);
		}
	}
}





