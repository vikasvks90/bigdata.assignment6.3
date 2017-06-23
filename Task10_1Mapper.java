/**
 * <h1>Task10_1Mapper</h1>
 * Mapper program to calculate the total units sold for each Company
 * This class will take input as (Key,Value) pair from a given file and will
 * produce the output as (Key,Value) pair.
 * */
package mapreduce.assignment6.task10;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Task10_1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	Text outKey = new Text();
	IntWritable outValue = new IntWritable();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] lineArray = value.toString().split("\\|");
		outKey.set(lineArray[0] + "\t" + lineArray[3]);
		outValue.set(1);
		context.write(outKey, outValue);
	}
}