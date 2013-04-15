package eu.deustotech.internet.ldclassifier.rdf2graph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RDF2Graph extends Configured implements Tool {

	public static void main(String[] args) {

		Configuration conf = new Configuration();
		try {
			int res = ToolRunner
					.run(conf,
							new eu.deustotech.internet.ldclassifier.rdf2graph.RDF2Graph(),
							args);
			System.exit(res);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {

		Configuration mainConf = super.getConf();
		mainConf.set("mapred.textoutputformat.separator", ",");
		Job job = new Job(mainConf);
		job.setJarByClass(RDF2Graph.class);
		job.setJobName("RDF2Graph");
		FileInputFormat.addInputPath(job, new Path("linkeddata/input"));
		TextOutputFormat.setOutputPath(job, new Path("linkeddata/output"));

		job.setMapperClass(SubjectMapper.class);
		job.setReducerClass(SubjectReducer.class);
		job.setNumReduceTasks(0); 
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);

		
		
		Configuration conf = super.getConf();
		conf.set("dataset", arg0[0]);
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
		
		Job objectExtractor = new Job(conf);
		objectExtractor.setInputFormatClass(KeyValueTextInputFormat.class);

		objectExtractor.setJarByClass(RDF2Graph.class);
		objectExtractor.setJobName("ObjectExtractor");
		
		
		KeyValueTextInputFormat.addInputPath(objectExtractor, new Path("linkeddata/output"));
		
		objectExtractor.setMapperClass(ObjectMapper.class);
		//objectExtractor.setReducerClass(ObjectReducer.class);
		// FileOutputFormat.setOutputPath(objectExtractor, new Path(
		// "linkeddata/objects"));

		//Scan scan = new Scan();
		// scan.setFilter(new FirstKeyOnlyFilter());
		objectExtractor.setOutputFormatClass(NullOutputFormat.class);

		// scan.addFamily(Bytes.toBytes("nodes"));
		// scan.addColumn(Bytes.toBytes("nodes"), Bytes.toBytes("class"));
		// scan.addColumn(Bytes.toBytes("nodes"), Bytes.toBytes("subjects"));
		/*TableMapReduceUtil.initTableMapperJob(arg0[0], scan,
				ObjectMapper.class, ImmutableBytesWritable.class, Result.class,
				objectExtractor);*/

		objectExtractor.setNumReduceTasks(0);

		objectExtractor.waitForCompletion(true);

		return 0;
	}
}
