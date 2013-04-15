package eu.deustotech.internet.ldclassifier.rdf2graph;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ObjectMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	public void map(Text key, Text value, Context context) {
		System.out.println(String.format("%s - %s", key, value.toString()));
		String subject = value.toString();
	}
	
}
