package eu.deustotech.internet.ldclassifier.filewriter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FileWriter {

	public static class VertexWriterMapper extends
			TableMapper<ImmutableBytesWritable, Text> {

		@Override
		public void map(ImmutableBytesWritable key, Result values,
				Context context) {
			String dataset = context.getConfiguration().get("dataset");			
			
			byte[] row = values.getRow();
			
			try {
				HTable table = new HTable(context.getConfiguration(), dataset);
				Get get = new Get(row);
				Result result = table.get(get);
				System.out.println(result);
				String nodeClass = new String(result.getValue(Bytes.toBytes("subdue"), Bytes.toBytes("class")));
				result = table.get(get);
				String id = new String(result.getValue(Bytes.toBytes("subdue"), Bytes.toBytes("id")));
				//System.out.println(nodeClass);
				String line = String.format("%s %s", id, nodeClass);
				System.out.println(line);
				context.write(new ImmutableBytesWritable(Bytes.toBytes(dataset.replace(".", ""))), new Text(line));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			
			/*String id = new String(values.getValue(Bytes.toBytes("subdue"),
					Bytes.toBytes("id")));
			String nodeClass = new String(values.getValue(
					Bytes.toBytes("subdue"), Bytes.toBytes("class")));

			String line = String.format("%s %s", id, nodeClass);

			System.out.println(line);

			try {
				context.write(new ImmutableBytesWritable(Bytes.toBytes(dataset)), new Text(line));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
		}
	}

	public static class VertexWriterReducer extends
			Reducer<ImmutableBytesWritable, Text, Text, Text> {

		@Override
		public void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context) {
			MultipleOutputs mos = new MultipleOutputs(context);
			//System.out.println(new String(key.get()));
			
			SortedMap<Long, String> vertexMap = new  ConcurrentSkipListMap<Long, String>();
			
			for (Text value : values) {
				String line = value.toString();
				
				long index = Long.valueOf(line.split(" ")[0]);
				String node = line.split(" ")[1];
				
				vertexMap.put(index, node);
			}
			
			for (Long index : vertexMap.keySet()) {
				try {
					mos.write(new String(key.get()).replace(".", " "), "v", new Text(String.format("%s \"%s\"", index.toString(), vertexMap.get(index))));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			/*for (Text value : values) {
				try {
					//mos.write(new Text("v"), value , new String(key.copyBytes()));
					//System.out.println(key.toString());
					mos.write(new String(key.get()), "v", value);
					//context.write(key, value);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}*/
		}
	}

	public static void run(String input, String output) {
		Configuration fileConfig = new Configuration();

		try {

			FileSystem fs = FileSystem.get(fileConfig);

			Set<String> datasets = getDatasets(input + "/part-r-00000", fs);

			for (String dataset : datasets) {

				fileConfig.set("dataset", dataset);
				Job fileJob = new Job(fileConfig);
				fileJob.setJarByClass(FileWriter.class);
				fileJob.setJobName(String.format("[LDClassifier]%s-WriterJob",
						dataset));

				// TextInputFormat.addInputPath(fileJob, new Path(input));

				Scan scan = new Scan();

				List<Filter> filters = new ArrayList<Filter>();

				Filter typeFilter = new ValueFilter(
						CompareFilter.CompareOp.EQUAL, new BinaryComparator(
								Bytes.toBytes("v")));
				filters.add(typeFilter);

				FilterList fl = new FilterList(
						FilterList.Operator.MUST_PASS_ALL, filters);

				scan.setFilter(fl);

				TableMapReduceUtil.initTableMapperJob(dataset, scan,
						VertexWriterMapper.class, ImmutableBytesWritable.class,
						Result.class, fileJob);

				fileJob.setOutputFormatClass(NullOutputFormat.class);
				
				FileOutputFormat.setOutputPath(fileJob, new Path(output + "/" + dataset.replace(".", "")));
				MultipleOutputs.addNamedOutput(fileJob, dataset.replace(".", ""),
						TextOutputFormat.class, Text.class, Text.class);

				fileJob.setMapOutputKeyClass(ImmutableBytesWritable.class);
				fileJob.setMapOutputValueClass(Text.class);
				fileJob.setOutputKeyClass(ImmutableBytesWritable.class);
				fileJob.setOutputValueClass(Text.class);
				
				fileJob.setMapperClass(VertexWriterMapper.class);
				fileJob.setReducerClass(VertexWriterReducer.class);
				
				
				fileJob.submit();

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

	private static Set<String> getDatasets(String input, FileSystem fs)
			throws FileNotFoundException, IOException {
		Set<String> datasets = new HashSet<String>();

		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(new Path(input))));

		// StringBuilder sb = new StringBuilder();
		String line = br.readLine();
		while (line != null) {
			String dataset = line.replace(" ", "");
			dataset = dataset.replace("\t", "");
			if (!datasets.contains(dataset)) {
				datasets.add(dataset);
				line = br.readLine();
			}
		}
		return datasets;
	}
}
