package eu.deustotech.internet.internet.ldclassifier.rdf2graph;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SubjectReducer extends Reducer<Text, Text, Text, LongWritable> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) {
		
		
		long count = 0;
		for (Text value : values) {
			count++;
		}
		
		try {
			context.write(key, new LongWritable(count));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
