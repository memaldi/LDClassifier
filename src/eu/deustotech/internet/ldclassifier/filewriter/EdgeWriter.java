package eu.deustotech.internet.ldclassifier.filewriter;

import java.io.IOException;
import java.util.Set;

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

public class EdgeWriter {
	
	public static class EdgeWriterMapper extends TableMapper<ImmutableBytesWritable, Text> {
		
		@Override
		public void map(ImmutableBytesWritable key, Result value, Context context){
			String dataset = context.getConfiguration().get("dataset");

			byte[] row = value.getRow();
			
			try {
				HTable table = new HTable(context.getConfiguration(), dataset);
				Get get = new Get(row);
				Result result = table.get(get);
				//System.out.println(result);
				String source = new String(result.getValue(
						Bytes.toBytes("subdue"), Bytes.toBytes("source")));
				result = table.get(get);
				String target = new String(result.getValue(
						Bytes.toBytes("subdue"), Bytes.toBytes("target")));
				result = table.get(get);
				String edge = new String(result.getValue(
						Bytes.toBytes("subdue"), Bytes.toBytes("edge")));
				
				String line = String.format("%s %s %s", source, target, edge);
				//System.out.println(line);
				context.write(
						new ImmutableBytesWritable(Bytes.toBytes(dataset
								.replace(".", ""))), new Text(line));
				table.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	public static class EdgeWriteReducer extends Reducer<ImmutableBytesWritable, Text, Text, Text> {
		
		@Override
		public void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context) {
			
			MultipleOutputs mos = new MultipleOutputs(context);
			
			for (Text value : values) {
				try {
					mos.write(new String(key.get()), "e", value);
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
				Job edgeJob = LaunchUtils.launch(input, output, "EdgeWriter", "e",
						dataset, fileConfig, EdgeWriterMapper.class, EdgeWriteReducer.class);
				edgeJob.submit();
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
