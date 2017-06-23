/**
 * <h1>Task10_1Reducer</h1>
 * Reducer program to calculate the total units sold for each Company
 * This class will take input as (Key,Value) pair from output of mapper class
 * value will be a combined list for all the values for a given key
 * */
package mapreduce.assignment6.task10;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task10_1Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	IntWritable outValue = new IntWritable();
	
	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		outValue.set(sum);
		context.write(key, outValue);
	}
}