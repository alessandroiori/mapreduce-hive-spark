package uniquelistener;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uniquelistener.Map.COUNTERS;


public class Main extends Configured implements Tool{
	
	private static org.apache.hadoop.mapreduce.Counters counters;
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 2) {
			System.out.println("Usage: uniquelisteners < in > < out >");
			System.exit(2);
		}
		
		Job job = Job.getInstance(getConf());
		job.setJobName("uniquelistener");
		job.setJarByClass(Main.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//return job.waitForCompletion(true) ? 0 : 1;
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
		counters = job.getCounters();
		
		return exitCode;
	}
	
	public static void main(String args[]) throws Exception{
		int exitCode = ToolRunner.run(new Main(), args);
		System.exit(exitCode);
		System.out.println("No. of Invalid Records :"
				 + counters.findCounter(COUNTERS.INVALID_RECORD_COUNT)
				 .getValue());
	}
}
