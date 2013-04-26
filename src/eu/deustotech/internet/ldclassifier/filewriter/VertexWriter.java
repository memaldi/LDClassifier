package eu.deustotech.internet.ldclassifier.filewriter;

import java.io.IOException;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class VertexWriter {

	public static class VertexWriterMapper extends
			TableMapper<ImmutableBytesWritable, Text> {

		@Override
		public void map(ImmutableBytesWritable key, Result value,
				Context context) {
			String dataset = context.getConfiguration().get("dataset");
			
			byte[] row = value.getRow();

			try {
				HTable table = new HTable(context.getConfiguration(), dataset);
				Get get = new Get(row);
				Result result = table.get(get);
				//System.out.println(result);
				String nodeClass = new String(result.getValue(
						Bytes.toBytes("subdue"), Bytes.toBytes("class")));
				result = table.get(get);
				String id = new String(result.getValue(Bytes.toBytes("subdue"),
						Bytes.toBytes("id")));
				// System.out.println(nodeClass);
				String line = String.format("%s %s", id, nodeClass);
				//System.out.println(line);
				context.write(
						new ImmutableBytesWritable(Bytes.toBytes(dataset
								.replace(".", ""))), new Text(line));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	public static class VertexWriterReducer extends
			Reducer<ImmutableBytesWritable, Text, Text, Text> {

		@Override
		public void reduce(ImmutableBytesWritable key, Iterable<Text> values,
				Context context) {
			
			MultipleOutputs mos = new MultipleOutputs(context);
			// System.out.println(new String(key.get()));

			SortedMap<Long, String> vertexMap = new ConcurrentSkipListMap<Long, String>();

			for (Text value : values) {
				String line = value.toString();

				long index = Long.valueOf(line.split(" ")[0]);
				String node = line.split(" ")[1];

				vertexMap.put(index, node);
			}

			for (Long index : vertexMap.keySet()) {
				try {
					mos.write(
							new String(key.get()).replace(".", " "),
							"v",
							new Text(String.format("%s \"%s\"",
									index.toString(), vertexMap.get(index))));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public static void run(String input, String output) {
		Configuration fileConfig = new Configuration();

		FileSystem fs;
		try {
			fs = FileSystem.get(fileConfig);
			Set<String> datasets = LaunchUtils.getDatasets(input + "/part-r-00000", fs);
			for (String dataset : datasets) {
				Job vertexJob = LaunchUtils.launch(input, output, "VertexWriter", "v",
						dataset, fileConfig, VertexWriterMapper.class, VertexWriterReducer.class);
				vertexJob.submit();
			}
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
