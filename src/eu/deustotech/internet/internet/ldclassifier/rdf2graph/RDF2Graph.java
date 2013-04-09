package eu.deustotech.internet.internet.ldclassifier.rdf2graph;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RDF2Graph extends Configured implements Tool {

	public static void main(String[] args) {
		
		Configuration conf = new Configuration();
		try {
			int res = ToolRunner.run(conf, new eu.deustotech.internet.internet.ldclassifier.rdf2graph.RDF2Graph(), args);
			System.exit(res);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		
		Job job = new Job(super.getConf());
		job.setJarByClass(RDF2Graph.class);
		job.setJobName("RDF2Graph");
		FileInputFormat.addInputPath(job, new Path("linkeddata/input"));
		FileOutputFormat.setOutputPath(job, new Path("linkeddata/output"));
		
		job.setMapperClass(SubjectMapper.class);
		job.setReducerClass(SubjectReducer.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.waitForCompletion(true);
		
		return 0;
	}
}
