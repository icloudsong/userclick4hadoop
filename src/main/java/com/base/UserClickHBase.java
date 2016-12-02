package com.base;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import net.sf.json.JSONObject;

/**
 * 从HDFS中读取用户点击日志，利用mapreduce并行进行用户点击数统计，统计结果存放在HBase数据库中
 * @author songbh
 *
 */
public class UserClickHBase {
	
	static class UserClickMapper extends Mapper<Object, Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private Text deviceId = new Text();
		
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			 JSONObject jsonobj = JSONObject.fromObject(value.toString());
			 String idfa = jsonobj.has("idfa") ? jsonobj.getString("idfa"):"";	
			 if (!idfa.equals("")) {
			    deviceId.set(idfa);
			    context.write(deviceId, one);
			 }
		}

	}
	
	static class UserClickReducer extends TableReducer<Text, IntWritable, NullWritable> {
		 public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException{  
	            int sum = 0;  
	            for(IntWritable i:values) {  
	                sum+=i.get();  
	            }  
	            Put put = new Put(Bytes.toBytes(key.toString()));  
	            // row, columnFamily:column,value = word, content:count, sum   
	            put.add(Bytes.toBytes("content"),Bytes.toBytes("count"),Bytes.toBytes(String.valueOf(sum)));  
	            context.write(NullWritable.get(), put);  
	    }  
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {  
   
        String tablename = "userclick";  
        Configuration conf = new Configuration();  
        conf.set(TableOutputFormat.OUTPUT_TABLE, tablename);  
        createHBaseTable(tablename);  
        Job job = new Job(conf,"WordCount table");  
        job.setJarByClass(UserClickHBase.class);  
        job.setNumReduceTasks(3);  
        job.setMapperClass(UserClickMapper.class);  
        job.setReducerClass(UserClickReducer.class);  
        job.setMapOutputKeyClass(Text.class);  
        job.setMapOutputValueClass(IntWritable.class);  
        job.setInputFormatClass(TextInputFormat.class);  
        job.setOutputFormatClass(TableOutputFormat.class);
        
        
        // 设置输入目录
        FileInputFormat.addInputPath(job, new Path(ConfigUtil.get("input")));
        System.exit(job.waitForCompletion(true)?0:1);  
    }
	
	public static void createHBaseTable(String tablename) throws IOException {  
        HTableDescriptor htd = new HTableDescriptor(tablename);  
        HColumnDescriptor col = new HColumnDescriptor("content");  
        htd.addFamily(col);  
        Configuration cfg = HBaseConfiguration.create();  
        HBaseAdmin admin = new HBaseAdmin(cfg);  
        if(admin.tableExists(tablename)) {  
            System.out.println("table exists,trying recreate table!");  
            admin.disableTable(tablename);  
            admin.deleteTable(tablename);  
            admin.createTable(htd);  
        }  
        else {  
            System.out.println("create new table:"+tablename);  
            admin.createTable(htd);  
        }  
    } 
	
     
}
