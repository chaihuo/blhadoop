package com.bl.hadoop.hdfs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

/**
 * 完成hdfs操作 
 */
public class TestHDFS {
	/**
	 * 读取hdfs文件
	 */
	@Test
	public void readFile() throws Exception{
		//注册url流处理器工厂(hdfs)
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		
		URL url = new URL("hdfs://10.62.250.31:8020/user/ubuntu/test");
		URLConnection conn = url.openConnection();
		InputStream is = conn.getInputStream();
		byte[] buf = new byte[is.available()];
		is.read(buf);
		is.close();
		String str = new String(buf);
		System.out.println(str);
	}
	
	/**
	 * 通过hadoop API访问文件
	 */
	@Test
	public void readFileByAPI() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://10.62.250.31:8020/");
		FileSystem fs = FileSystem.get(conf) ;
		Path p = new Path("/user/ubuntu/test");
		FSDataInputStream fis = fs.open(p);
		byte[] buf = new byte[1024];
		int len = -1 ; 
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		while((len = fis.read(buf)) != -1){
			baos.write(buf, 0, len);
		}
		fis.close();
		baos.close();
		System.out.println(new String(baos.toByteArray()));
	}
	
	/**
	 * 通过hadoop API访问文件
	 */
	@Test
	public void readFileByAPI2() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://10.62.250.31:8020/");
		FileSystem fs = FileSystem.get(conf) ;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		Path p = new Path("/user/ubuntu/test");
		FSDataInputStream fis = fs.open(p);
		IOUtils.copyBytes(fis, baos, 1024);
		System.out.println(new String(baos.toByteArray()));
	}
	
	/**
	 * mkdir
	 */
	@Test
	public void mkdir() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://10.62.250.31:8020/");
		FileSystem fs = FileSystem.get(conf) ;
		fs.mkdirs(new Path("/user/ubuntu/myhadoop"));
	}
	
	/**
	 * putFile
	 */
	@Test
	public void putFile() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://10.62.250.31:8020/");
		FileSystem fs = FileSystem.get(conf) ;
		FSDataOutputStream out = fs.create(new Path("/user/ubuntu/myhadoop/a.txt"));
		out.write("helloworld".getBytes());
		out.close();
	}
	
	/**
	 * removeFile
	 */
	@Test
	public void removeFile() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://10.62.250.31:8020/");
		FileSystem fs = FileSystem.get(conf) ;
		Path p = new Path("/user/ubuntu/myhadoop");
		fs.delete(p, true);
	}


	/**
	 * list directory
	 */
	@Test
	public void listDirectory() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://10.62.250.31:8020/");
		FileSystem fs = FileSystem.get(conf) ;
		Path p = new Path("/user/ubuntu");
		FileStatus[] f = fs.listStatus(p);
		for (FileStatus fileStatus:f
		){
			System.out.println(new String(fileStatus.getPath().toString()));
			System.out.println(new String("\r\n"));
		}
	}




}
