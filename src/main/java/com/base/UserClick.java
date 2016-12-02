package com.base;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.base.WordCount.WordCountMapper;
import com.base.WordCount.WordCountReducer;

import net.sf.json.JSONObject;

/**
 * 从HDFS中读取用户点击日志，利用mapreduce并行进行用户点击数统计，统计结果存放在HDFS中
 * @author songbh
 *
 */
public class UserClick {
	
	static class UserClickMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text deviceId = new Text();
		
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			 JSONObject jsonobj = JSONObject.fromObject(value.toString());
			 String idfa = jsonobj.has("idfa") ? jsonobj.getString("idfa"):"";	
			 
			 deviceId.set(idfa);
			 context.write(deviceId, one);
		}

	}
	
	static class UserClickReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		private Text deviceId = new Text();
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			      throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val: values) {
				sum += val.get();
			}
			result.set(sum);
			deviceId.set("用户点击数:"+key.toString());
			context.write(deviceId, result);
		}
	}
	
   public static void main(String[] args) throws Exception {
		
		String input = ConfigUtil.get("input");
		String output = ConfigUtil.get("output");
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "userclick");
		
		job.setJarByClass(UserClick.class);
		job.setMapperClass(UserClickMapper.class);
		job.setReducerClass(UserClickReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
