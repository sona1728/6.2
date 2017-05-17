Use key value output format to save the output of Session 6 
–
Assignment 1(Travel Data analysis) and save the output as comma (,) separated instead of(\t) separated.




//driver class

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class driver61 {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator",",");
		Job job = new Job(conf, "DemoTask1");
		job.setJarByClass(driver61.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(map61.class);
		job.setReducerClass(reduce61.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		
		
		job.waitForCompletion(true);// wait for the job to end
		
		Job job1 = new Job(conf, "DemoTask2");
		conf.set("mapred.textoutputformat.separator",",");
		job1.setJarByClass(driver61.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		job1.setMapperClass(map61b.class);
		job1.setReducerClass(reduce61.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job1, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job1,new Path(args[2]));
		
		
		
		job1.waitForCompletion(true);// wait for the job to end

		
		Job job2 = new Job(conf, "DemoTask3");
		conf.set("mapred.textoutputformat.separator",",");
		job2.setJarByClass(driver61.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		job2.setMapperClass(map61c.class);
		job2.setReducerClass(reduce61.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job2, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job2,new Path(args[3]));
		
		
		
		job2.waitForCompletion(true);// wait for the job to end

		
			}}

//map for statement 1
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
public class map61 extends Mapper<LongWritable , Text, Text , IntWritable>{
	IntWritable no = new IntWritable(1);
	public void map(LongWritable key , Text value , Context context) throws IOException, InterruptedException
	{
		String[] l = value.toString().split("\\s+");// splitting with space
		int a = 0;
		if(l.length>0)
		{
		String s = l[2] ;// considering the second element destination
		if(s.equals("MIA") || s.equals("MCO"))
		{
			a = Integer.parseInt(l[4])+Integer.parseInt(l[5])+Integer.parseInt(l[6])+Integer.parseInt(l[7])+Integer.parseInt(l[8]);
			context.write(new Text("Number of passenger to MIA and MCO are:"), new IntWritable(a));// input to the reduce class 
		}
	}
	}
}

//map class for statement 2


import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
public class map61b extends Mapper<LongWritable , Text, Text , IntWritable>{
	IntWritable no = new IntWritable(1);
	public void map(LongWritable key , Text value , Context context) throws IOException, InterruptedException
	{
		String[] l = value.toString().split("\\s+");
		int a =0;
		if(l.length>0)
		{
		String s = l[1];
		if(s.equals("MIA") || s.equals("HOU"))
		{
			a = Integer.parseInt(l[4])+Integer.parseInt(l[5])+Integer.parseInt(l[6])+Integer.parseInt(l[7])+Integer.parseInt(l[8]);
			context.write(new Text("Number of passenger from MIA and HOU are:"), new IntWritable(a));
		}
	}
	}
}


//map class for statement 3

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
public class map61c extends Mapper<LongWritable , Text, Text , IntWritable>{
	IntWritable no = new IntWritable(1);
	public void map(LongWritable key , Text value , Context context) throws IOException, InterruptedException
	{
		String[] l = value.toString().split("\\s+");
		int a = 0;
		if(l.length>0)
		{
		String s = l[2];
		int s1 = Integer.parseInt(l[3]);
		if(s1==1 || s1==3 || s1==5 || s1==7)
		if(s.equals("LAS") || s.equals("LAX"))
		{
			a = Integer.parseInt(l[4])+Integer.parseInt(l[5])+Integer.parseInt(l[6])+Integer.parseInt(l[7])+Integer.parseInt(l[8]);
			context.write(new Text("Number of passenger to LAS and LAX are:"), new IntWritable(a));
		}
	}
	}
}

//reducer class

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
public class reduce61 extends Reducer<Text,IntWritable,Text, IntWritable> {
	public void reduce(Text key, Iterable<IntWritable> value , Context con) throws IOException, InterruptedException
	{
		int sum =0;
		for(IntWritable v : value)
		{
			sum = sum + v.get();
		}
		con.write(key, new IntWritable(sum));
	}

}


//output
~/Desktop$ hadoop fs -cat /user/sona/output/61aop2/part-r-00000
Number of passenger to MIA and MCO are:,611
:~/Desktop$ hadoop fs -cat /user/sona/output/61bop2/part-r-00000
Number of passenger from MIA and HOU are:,517
~/Desktop$ hadoop fs -cat /user/sona/output/61cop2/part-r-00000
Number of passenger to LAS and LAX are:,362
Thus generated code with comma separated values
