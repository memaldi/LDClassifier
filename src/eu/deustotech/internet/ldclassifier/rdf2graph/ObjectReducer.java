package eu.deustotech.internet.ldclassifier.rdf2graph;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ObjectReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) {
		for (Text value : values) {
			System.out.println(value);
		}
	}
}
