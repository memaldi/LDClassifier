package eu.deustotech.internet.ldclassifier.edgegenerator;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EdgeGenerator {
	
	public static class EdgeGeneratorReducer extends Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) {
			
			try {
				//System.out.println(key.toString());
				context.write(key, new Text(""));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static class EdgeGeneratorMapper extends
			Mapper<Text, Text, Text, Text> {

		static enum counter {
			EDGE_COUNTER
		}

		@Override
		public void map(Text key, Text value, Context context) {

			try {
				Configuration config = HBaseConfiguration.create();
				HTable table = new HTable(config, "datasets");

				Get get = new Get(Bytes.toBytes(value.toString()));
				Result r = table.get(get);

				String subject = value.toString();

				HTable sTable = new HTable(config, key.toString());
				Get sGet = new Get(Bytes.toBytes(value.toString()));
				Result sr = sTable.get(sGet);
				String id = new String(sr.getValue(Bytes.toBytes("subdue"),
						Bytes.toBytes("id")));

				//System.out.println(id + "-" + subject);

				NavigableMap<byte[], byte[]> objects = r.getFamilyMap(Bytes
						.toBytes("o"));
				NavigableMap<byte[], byte[]> properties = r.getFamilyMap(Bytes
						.toBytes("p"));

				for (byte[] objectKey : objects.keySet()) {
					String object = new String(objects.get(objectKey));
					String property = new String(properties.get(objectKey));

					//System.out.println(object);
					
					if ("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
							.equals(property)) {
						Put p = new Put(Bytes.toBytes(subject));
						p.add(Bytes.toBytes("subdue"), Bytes.toBytes("class"),
								Bytes.toBytes(object));
						sTable.put(p);
					} else {
						
						Get oGet = new Get(Bytes.toBytes(object));
						Result or = sTable.get(oGet);
						if (!or.isEmpty()) {
							String oId = new String(or.getValue(
									Bytes.toBytes("subdue"),
									Bytes.toBytes("id")));
							long count = context.getCounter(counter.EDGE_COUNTER).getValue();
							context.getCounter(counter.EDGE_COUNTER).increment(1);
							Put p = new Put(Bytes.toBytes(String.valueOf(count)));
							p.add(Bytes.toBytes("subdue"), Bytes.toBytes("source"), Bytes.toBytes(id));
							p.add(Bytes.toBytes("subdue"), Bytes.toBytes("target"), Bytes.toBytes(oId));
							p.add(Bytes.toBytes("subdue"), Bytes.toBytes("edge"), Bytes.toBytes(property));
							p.add(Bytes.toBytes("subdue"), Bytes.toBytes("type"), Bytes.toBytes("e"));	
							
							sTable.put(p);
							//context.write(new Text(), new Text());
						}
					}
				}
				
				context.write(key, key);
				
			} catch (IOException e) { // TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	public static void run(String input, String output) {

		try {
			Configuration edgeConfig = new Configuration();
			edgeConfig
					.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",
							",");

			Job edgeJob = new Job(edgeConfig);

			edgeJob.setJarByClass(EdgeGenerator.class);
			edgeJob.setJobName("[LDClassifier]EdgeJob");
			KeyValueTextInputFormat.addInputPath(edgeJob, new Path(input));
			edgeJob.setInputFormatClass(KeyValueTextInputFormat.class);
			FileOutputFormat.setOutputPath(edgeJob, new Path(output));
			//edgeJob.setOutputFormatClass(NullOutputFormat.class);

			edgeJob.setMapOutputKeyClass(Text.class);
			edgeJob.setMapOutputValueClass(Text.class);
			edgeJob.setOutputKeyClass(Text.class);
			edgeJob.setOutputValueClass(Text.class);
			
			edgeJob.setMapperClass(EdgeGeneratorMapper.class);
			edgeJob.setReducerClass(EdgeGeneratorReducer.class);
			edgeJob.setNumReduceTasks(1);
			edgeJob.waitForCompletion(true);
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
}
