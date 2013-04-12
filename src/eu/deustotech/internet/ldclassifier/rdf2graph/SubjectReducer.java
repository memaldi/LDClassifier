package eu.deustotech.internet.ldclassifier.rdf2graph;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SubjectReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) {
		System.out.println("Reducing");
		for (Text value : values) {
			System.out.println(String.format("%s-%s", key, value));
		}

	}
}
