package eu.deustotech.internet.ldclassifier.loader;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TripleLoader {

	static enum counter {
		COUNTER, PROGRESS
	}

	public static class TripleLoaderMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) {
			try {
				String graph = ((FileSplit) context.getInputSplit()).getPath()
						.getName();
				String[] triple = value.toString().split(" ");
				String subject = triple[0];
				String predicate = triple[1];
				String object = "";
				if (triple[2].startsWith("<")) {
					object = triple[2];
					Configuration config = HBaseConfiguration.create();

					HTable table = new HTable(config, "datasets");
					
					Put p = new Put(Bytes.toBytes(String.valueOf(subject)));
					long count = context.getCounter(counter.COUNTER).getValue();
					context.getCounter(counter.COUNTER).increment(1);
					
					p.add(Bytes.toBytes("p"),
							Bytes.toBytes(String.valueOf(count)),
							Bytes.toBytes(predicate));
					p.add(Bytes.toBytes("o"),
							Bytes.toBytes(String.valueOf(count)),
							Bytes.toBytes(object));
					p.add(Bytes.toBytes("g"), Bytes.toBytes(""),
							Bytes.toBytes(graph));
					table.put(p);
							
					if ("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>".equals(predicate)) {
						HTable dTable = TripleLoaderReducer.getTable(new Text(graph));
						Put dP = new Put(Bytes.toBytes(String.valueOf(subject)));
						dP.add(Bytes.toBytes("subdue"), Bytes.toBytes("class"), Bytes.toBytes(object));
						dP.add(Bytes.toBytes("subdue"), Bytes.toBytes("type"), Bytes.toBytes("v"));
						dTable.put(dP);
						dTable.close();
					}
					
					context.write(new Text(graph), new Text(subject));
					table.close();
				}
				context.getCounter(counter.PROGRESS).increment(1);
				context.progress();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	public static class TripleLoaderReducer extends
			Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) {

			try {
				context.write(key, new Text(""));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		public static HTable getTable(Text key) throws IOException {
			Configuration config = HBaseConfiguration.create();
			HTable table = null;
			HBaseAdmin hbase = null;
			try {
				hbase = new HBaseAdmin(config);
				table = new HTable(config, key.toString());
			} catch (MasterNotRunningException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ZooKeeperConnectionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("Creating table " + key.toString());
				HTableDescriptor desc = new HTableDescriptor(key.toString());
				HColumnDescriptor subdue = new HColumnDescriptor("subdue");
				desc.addFamily(subdue);

				hbase.createTable(desc);
				table = new HTable(config, key.toString());
			}

			return table;
		}
	}

	public static void run(String input, String dataset) {

		Configuration loadConfig = new Configuration();
		//loadConfig.set("mapred.textoutputformat.separator", ",");
		try {

			preLoad();

			Job loadJob = new Job(loadConfig);
			loadJob.setJarByClass(TripleLoader.class);
			loadJob.setJobName("[LDClassifier]LoadJob-" + dataset);
			FileInputFormat.addInputPath(loadJob, new Path(input));
			//TextOutputFormat.setOutputPath(loadJob, new Path(output));
			loadJob.setOutputFormatClass(NullOutputFormat.class);
			
			loadJob.setMapOutputKeyClass(Text.class);
			loadJob.setMapOutputValueClass(Text.class);
			//loadJob.setOutputKeyClass(Text.class);
			//loadJob.setOutputValueClass(Text.class);

			loadJob.setMapperClass(TripleLoaderMapper.class);
			//loadJob.setReducerClass(TripleLoaderReducer.class);

			loadJob.waitForCompletion(true);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void preLoad() throws IOException {
		Configuration config = HBaseConfiguration.create();
		HBaseAdmin hbase = new HBaseAdmin(config);
		try {
			@SuppressWarnings("unused")
			HTable table = new HTable(config, "datasets");
		} catch (IOException e) {
			System.out.println("Creating table...");
			HTableDescriptor desc = new HTableDescriptor("datasets");
			HColumnDescriptor predicate = new HColumnDescriptor("p".getBytes());
			HColumnDescriptor object = new HColumnDescriptor("o".getBytes());
			HColumnDescriptor graph = new HColumnDescriptor("g".getBytes());

			desc.addFamily(predicate);
			desc.addFamily(object);
			desc.addFamily(graph);

			hbase.createTable(desc);
		}
	}

}
