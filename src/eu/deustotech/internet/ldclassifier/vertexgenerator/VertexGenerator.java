package eu.deustotech.internet.ldclassifier.vertexgenerator;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import eu.deustotech.internet.ldclassifier.filewriter.LaunchUtils;

public class VertexGenerator {

	public static class VertexGeneratorMapper extends TableMapper<ImmutableBytesWritable, Text> {
		
		static enum counter { VERTEX_COUNTER }
		
		@Override
		public void map(ImmutableBytesWritable key, Result value, Context context) {
			String dataset = context.getConfiguration().get("dataset");
			
			byte[] row = value.getRow();
			
			try {
				HTable table = new HTable(context.getConfiguration(), dataset);
				Put p = new Put(row);
				long count = context.getCounter(counter.VERTEX_COUNTER)
						.getValue() + 1;
				context.getCounter(counter.VERTEX_COUNTER).increment(1);
				p.add(Bytes.toBytes("subdue"), Bytes.toBytes("id"), Bytes.toBytes(String.valueOf(count)));
				table.put(p);
				table.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
		
	public static void run(String input) {
		Configuration fileConfig = new Configuration();

		FileSystem fs;
		try {
			fs = FileSystem.get(fileConfig);
			Set<String> datasets = LaunchUtils.getDatasets(input + "/part-r-00000", fs);
			for (String dataset : datasets) {
				Job vertexJob = LaunchUtils.launch(input, null, "VertexGenerator", null,
						dataset, fileConfig, VertexGeneratorMapper.class, null);
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
