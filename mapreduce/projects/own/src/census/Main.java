package census;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Configured object allows to specify parameters of mapreduce
 * Tool interface allows specification parameters on command line 
 * @author ambpl
 *
 */
public class Main extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJobName("average");
		job.setJarByClass(Main.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1; // 0 success
	}
	
	/**
	 * entry point method for run mapreduce 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Main(), args);
		System.exit(exitCode);
	}
}
