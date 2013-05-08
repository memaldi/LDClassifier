package eu.deustotech.internet.ldclassifier.multigraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
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

import eu.deustotech.internet.ldclassifier.loader.TripleLoader.TripleLoaderReducer;

public class MultiGraph {

	public static class MultiGraphMapper extends
			TableMapper<ImmutableBytesWritable, Text> {

		static enum counter { VERTEX_COUNTER }
		
		@Override
		public void map(ImmutableBytesWritable key, Result value,
				Context context) {
			//System.out.println("Maaaaap!");
			byte[] row = value.getRow();
			//System.out.print(new String(row));
			try {
				HTable table = TripleLoaderReducer
						.getTable(new Text("datasets"));

				HTable dTable = TripleLoaderReducer.getTable(new Text(context
						.getConfiguration().get("dataset")));

				Get get = new Get(row);

				Result result = table.get(get);

				long count = context.getCounter(counter.VERTEX_COUNTER).getValue();
				context.getCounter(counter.VERTEX_COUNTER).increment(1);
				
				
				String subgraph = String.format("t # %s\n", count);
				subgraph += String.format(
						"v 0 %s\n",
						new String(value.getValue(Bytes.toBytes("subdue"),
								Bytes.toBytes("class"))));

				NavigableMap<byte[], byte[]> objectMap = result
						.getFamilyMap(Bytes.toBytes("o"));
				NavigableMap<byte[], byte[]> propertyMap = result
						.getFamilyMap(Bytes.toBytes("p"));

				List<String> edgeList = new ArrayList<String>();

				int i = 1;

				for (byte[] objectKey : objectMap.keySet()) {
					byte[] object = objectMap.get(objectKey);
					Get dGet = new Get(object);
					Result dResult = dTable.get(dGet);

					if (!dResult.isEmpty()) {
						subgraph += String.format(
								"v %s %s\n",
								i,
								new String(dResult.getValue(
										Bytes.toBytes("subdue"),
										Bytes.toBytes("class"))));
						edgeList.add(String.format("e 0 %s %s\n", i, new String(
								propertyMap.get(objectKey))));
						i++;
					}
				}
				
				for (String edge : edgeList) {
					subgraph += edge;
				}
				//System.out.println(subgraph);
				context.write(new ImmutableBytesWritable(Bytes.toBytes(context
						.getConfiguration().get("dataset"))), new Text(subgraph));
				table.close();
				dTable.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}
	
	public static class MultiGraphReducer extends Reducer<ImmutableBytesWritable, Text, Text, Text> {
		
		@Override
		public void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context){
			//System.out.println("Reduceee!");
			for (Text value : values) {
				try {
					//System.out.println(value.toString());
					context.write(null, value);
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

	public static void run(String dataset, String output) {
		Configuration config = new Configuration();
		config.set("dataset", dataset);
		Job fileJob;
		try {
			fileJob = new Job(config);
			fileJob.setJarByClass(MultiGraph.class);
			fileJob.setJobName(String.format("[LDClassifier]%s-%s", dataset,
					"MultiGraph"));

			Scan scan = new Scan();

			/*List<Filter> filters = new ArrayList<Filter>();

			Filter typeFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
					new BinaryComparator(Bytes.toBytes(dataset)));
			filters.add(typeFilter);

			FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL,
					filters);

			scan.setFilter(fl); */

			TableMapReduceUtil.initTableMapperJob(dataset, scan,
					MultiGraphMapper.class, ImmutableBytesWritable.class,
					Text.class, fileJob);
			FileOutputFormat.setOutputPath(fileJob, new Path(output + "/"
					+ dataset.replace(".", "")));

			fileJob.setMapOutputKeyClass(ImmutableBytesWritable.class);
			fileJob.setMapOutputValueClass(Text.class);
			fileJob.setOutputKeyClass(Text.class);
			fileJob.setOutputValueClass(Text.class);

			fileJob.setMapperClass(MultiGraphMapper.class);
			fileJob.setReducerClass(MultiGraphReducer.class);

			fileJob.waitForCompletion(true);

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
