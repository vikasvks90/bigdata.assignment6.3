/**
 * <h1>Task10_2</h1>
 * This class will be used to define the configuration components for the given task
 * */
package mapreduce.assignment6.task10;

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.io.Text;

public class Task10_2 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Task10_2");
		job.setJarByClass(Task10_2.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StateSales.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StateSales.class);
		job.setMapperClass(Task10_2Mapper.class);
		job.setReducerClass(Task10_2Reducer.class);
				
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}