package eu.deustotech.internet.ldclassifier.metis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GraphFileGenerator {

	public static void run(String dataset, String output) {
		Configuration config = new Configuration();
		config.set("dataset", dataset);
		try {
			Job job = new Job(config);
			job.setJarByClass(GraphFileGenerator.class);
			job.setJobName(String.format("[LDClassifier]%s-Metis", dataset));

			Scan scan = new Scan();

			Filter filter = new SingleColumnValueFilter(
					Bytes.toBytes("subdue"), Bytes.toBytes("type"),
					CompareOp.EQUAL, Bytes.toBytes("v"));
			scan.setFilter(filter);

			TableMapReduceUtil.initTableMapperJob(dataset, scan,
					GraphFileGeneratorMapper.class,
					ImmutableBytesWritable.class, Result.class, job);
			FileOutputFormat.setOutputPath(job,
					new Path(output + "/" + dataset.replace(".", "")));

			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(ImmutableBytesWritable.class);
			job.setOutputValueClass(Text.class);
			
			job.waitForCompletion(true);
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

	public static class GraphFileGeneratorReducer extends
			Reducer<ImmutableBytesWritable, Text, Text, Text> {

		@Override
		public void reduce(ImmutableBytesWritable key, Iterable<Text> values,
				Context context) {

			System.out.println("Reduuuce");
			SortedMap<Long, String> edgeMap = new ConcurrentSkipListMap<Long, String>();

			for (Text value : values) {
				String stringValue = value.toString();
				long nodeID = Long.parseLong(String.valueOf(stringValue
						.charAt(0)));
				String edges = stringValue.substring(1,
						stringValue.length() - 1);
				System.out.println("nodeID: " + nodeID);
				System.out.println("edges: " + edges);
				edgeMap.put(nodeID, edges);
			}

			for (long nodeID : edgeMap.keySet()) {
				try {
					System.out.println(edgeMap.get(nodeID));
					context.write(new Text(""), new Text(edgeMap.get(nodeID) + "\n"));
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

	public static class GraphFileGeneratorMapper extends
			TableMapper<ImmutableBytesWritable, Text> {

		@Override
		public void map(ImmutableBytesWritable key, Result value,
				Context context) {

			String dataset = context.getConfiguration().get("dataset");
			try {
				byte[] vertexID = value.getValue(Bytes.toBytes("subdue"),
						Bytes.toBytes("id"));
				HTable table = new HTable(context.getConfiguration(), dataset);
				Scan scan = new Scan();
				List<Filter> filterList = new ArrayList<Filter>();
				Filter edgeFilter = new SingleColumnValueFilter(
						Bytes.toBytes("subdue"), Bytes.toBytes("type"),
						CompareOp.EQUAL, Bytes.toBytes("e"));
				Filter sourceFilter = new SingleColumnValueFilter(
						Bytes.toBytes("subdue"), Bytes.toBytes("source"),
						CompareOp.EQUAL, vertexID);
				filterList.add(edgeFilter);
				filterList.add(sourceFilter);
				FilterList fl = new FilterList(Operator.MUST_PASS_ALL,
						filterList);
				scan.setFilter(fl);
				ResultScanner rs = table.getScanner(scan);
				Result edgeResult;
				String edgeList = new String(vertexID);
				//System.out.println(edgeList);
				while ((edgeResult = rs.next()) != null) {
					byte[] edgeTarget = edgeResult.getValue(
							Bytes.toBytes("subdue"), Bytes.toBytes("target"));
					
					//System.out.println(String.format(" %s", new String(edgeTarget)));
					
					edgeList += String.format(" %s", new String(edgeTarget));
				}
				
				context.write(key, new Text(edgeList));

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
