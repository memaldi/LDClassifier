package eu.deustotech.internet.ldclassifier.splitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
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

public class GraphSplitter {

	public static class GraphSplitterMapper extends
			TableMapper<ImmutableBytesWritable, Text> {
		@Override
		public void map(ImmutableBytesWritable key, Result value,
				Context context) {

			// System.out.println(Bytes.toLong(value.getValue(
			// Bytes.toBytes("subdue"), Bytes.toBytes("id"))));

			String nodeClass = new String(value.getValue(
					Bytes.toBytes("subdue"), Bytes.toBytes("class")));
			long id = Bytes.toLong(value.getValue(Bytes.toBytes("subdue"),
					Bytes.toBytes("id")));
			String line = String.format("%s %s", id, nodeClass);

			String dataset = context.getConfiguration().get("dataset");
			String offset = context.getConfiguration().get("offset");
			String limit = context.getConfiguration().get("limit");
			String keyName = String.format("[%s]%s-%s",
					dataset.replace(".", ""), offset, limit);

			//System.out.println(String.format("%s - %s", keyName, line));
			
			try {
				context.write(
						new ImmutableBytesWritable(Bytes.toBytes(keyName)),
						new Text(line));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static class GraphSplitterReducer extends
			Reducer<ImmutableBytesWritable, Text, Text, Text> {

		public void reduce(ImmutableBytesWritable key, Iterable<Text> values,
				Context context) {
			
			Set<String> edgeSet = new HashSet<String>();
			
			String dataset = context.getConfiguration().get("dataset");
			String limit = context.getConfiguration().get("limit");
			try {
				HTable table = new HTable(context.getConfiguration(), dataset);
				
				for (Text value : values) {
					long id = Long.parseLong(value.toString().split(" ")[0]);
					System.out.println(id);
					Scan scan = new Scan();
					List<Filter> idFilterList = new ArrayList<Filter>();
					List<Filter> filterList = new ArrayList<Filter>();
					
					Filter sourceIdFilter = new SingleColumnValueFilter(
							Bytes.toBytes("subdue"), Bytes.toBytes("source"),
							CompareFilter.CompareOp.EQUAL,
							Bytes.toBytes(id));
					
					Filter targetIdFilter = new SingleColumnValueFilter(
							Bytes.toBytes("subdue"), Bytes.toBytes("target"),
							CompareFilter.CompareOp.EQUAL,
							Bytes.toBytes(id));
					
					idFilterList.add(sourceIdFilter);
					idFilterList.add(targetIdFilter);
					
					FilterList idfl = new FilterList(FilterList.Operator.MUST_PASS_ONE, idFilterList);					
					
					Filter edgeFilter = new SingleColumnValueFilter(
							Bytes.toBytes("subdue"), Bytes.toBytes("type"),
							CompareFilter.CompareOp.EQUAL, Bytes.toBytes("e"));
					
					Filter sourceFilter = new SingleColumnValueFilter(
							Bytes.toBytes("subdue"), Bytes.toBytes("source"),
							CompareFilter.CompareOp.LESS_OR_EQUAL,
							Bytes.toBytes(Long.parseLong(limit)));
					
					Filter targetFilter = new SingleColumnValueFilter(
							Bytes.toBytes("subdue"), Bytes.toBytes("target"),
							CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(Long
									.parseLong(limit)));
	
					filterList.add(sourceFilter);
					filterList.add(targetFilter);
					
					FilterList constraintfl = new FilterList(FilterList.Operator.MUST_PASS_ALL, filterList);
										
					FilterList fl = new FilterList();
					fl.addFilter(edgeFilter);
					//fl.addFilter(idfl);
					//fl.addFilter(constraintfl);
					
					scan.setFilter(fl);
					
					ResultScanner rs = table.getScanner(scan);
					
					Result rr;
					while((rr = rs.next()) != null) {
						
						long source = Bytes.toLong(rr.getValue(
								Bytes.toBytes("subdue"), Bytes.toBytes("source")));
						long target = Bytes.toLong(rr.getValue(
								Bytes.toBytes("subdue"), Bytes.toBytes("target")));
						String edge = new String(rr.getValue(
								Bytes.toBytes("subdue"), Bytes.toBytes("edge")));
						if (source == id || target == id) {
							System.out.println(rr);
							System.out.println(String.format("%s - %s - %s", source, target, edge));
						}
						
						
						/*String edgeStr = String.format("e %s %s \"%s\"", source, target, edge);
						edgeSet.add(edgeStr);*/
						
					}
					rs.close();
					
					context.write(new Text("v"), value);
					
				}
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

	public static void run(String dataset, String offset, String limit,
			String output) {
		Configuration config = new Configuration();
		config.set("dataset", dataset);
		config.set("offset", offset);
		config.set("limit", limit);
		try {
			Job job = new Job(config);
			job.setJarByClass(GraphSplitter.class);
			job.setJobName(String.format("[LDClassifier]%s-splitter", dataset));

			Scan scan = new Scan();
			List<Filter> filterList = new ArrayList<Filter>();

			Filter offsetFilter = new SingleColumnValueFilter(
					Bytes.toBytes("subdue"), Bytes.toBytes("id"),
					CompareFilter.CompareOp.GREATER_OR_EQUAL,
					Bytes.toBytes(Long.parseLong(offset)));
			Filter limitFilter = new SingleColumnValueFilter(
					Bytes.toBytes("subdue"), Bytes.toBytes("id"),
					CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(Long
							.parseLong(limit)));

			Filter vertexFilter = new SingleColumnValueFilter(
					Bytes.toBytes("subdue"), Bytes.toBytes("type"),
					CompareFilter.CompareOp.EQUAL, Bytes.toBytes("v"));
			filterList.add(vertexFilter);
			filterList.add(limitFilter);
			filterList.add(offsetFilter);
			FilterList fl = new FilterList(Operator.MUST_PASS_ALL, filterList);
			scan.setFilter(fl);

			TableMapReduceUtil.initTableMapperJob(dataset, scan,
					GraphSplitterMapper.class, ImmutableBytesWritable.class,
					Result.class, job);

			FileOutputFormat.setOutputPath(job,
					new Path(output + "/" + dataset.replace(".", "")));

			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(ImmutableBytesWritable.class);
			job.setOutputValueClass(Text.class);

			job.setReducerClass(GraphSplitterReducer.class);
			
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
}
