package com.base;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
	
	static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		protected void map(Object key, Text value, Context context)
		    throws IOException, InterruptedException {
			StringTokenizer tk = new StringTokenizer(value.toString());
			while(tk.hasMoreElements()) {
				String tmp = tk.nextToken();
				word.set(tmp);
				context.write(word, one);
			}
		}
		
	}
	
	
	static class  WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable result = new IntWritable();
		private Text keyEx = new Text();
		
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)
		      throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val: values) {
				sum += val.get();
			}
			result.set(sum);
			keyEx.set(key.toString());
			context.write(keyEx, result);
		}
		
	}

	
	public static void main(String[] args) throws Exception {
		
		String input = ConfigUtil.get("input");
		String output = ConfigUtil.get("output");
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "mywordcount");
		
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
