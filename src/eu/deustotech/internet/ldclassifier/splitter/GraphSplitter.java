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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

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

			// System.out.println(String.format("%s - %s", keyName, line));

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

			MultipleOutputs<Text, Text> mos = new MultipleOutputs<Text, Text>(context);
			
			String dataset = context.getConfiguration().get("dataset");
			long limit = context.getConfiguration().getLong("limit", 0);
			String namedOutput = context.getConfiguration().get("namedOutput");
			//System.out.println(String.format("NamedOutput: %s", namedOutput));
			//System.out.println(String.format("Limit: %s", limit));
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
							CompareFilter.CompareOp.EQUAL, Bytes.toBytes(id));

					Filter targetIdFilter = new SingleColumnValueFilter(
							Bytes.toBytes("subdue"), Bytes.toBytes("target"),
							CompareFilter.CompareOp.EQUAL, Bytes.toBytes(id));

					idFilterList.add(sourceIdFilter);
					idFilterList.add(targetIdFilter);

					FilterList idfl = new FilterList(
							FilterList.Operator.MUST_PASS_ONE, idFilterList);

					Filter edgeFilter = new SingleColumnValueFilter(
							Bytes.toBytes("subdue"), Bytes.toBytes("type"),
							CompareFilter.CompareOp.EQUAL, Bytes.toBytes("e"));

					Filter sourceFilter = new SingleColumnValueFilter(
							Bytes.toBytes("subdue"), Bytes.toBytes("source"),
							CompareFilter.CompareOp.LESS_OR_EQUAL,
							Bytes.toBytes(limit));

					Filter targetFilter = new SingleColumnValueFilter(
							Bytes.toBytes("subdue"), Bytes.toBytes("target"),
							CompareFilter.CompareOp.LESS_OR_EQUAL,
							Bytes.toBytes(limit));

					filterList.add(sourceFilter);
					filterList.add(targetFilter);

					FilterList constraintfl = new FilterList(
							FilterList.Operator.MUST_PASS_ALL, filterList);

					FilterList fl = new FilterList();
					fl.addFilter(edgeFilter);
					fl.addFilter(idfl);
					fl.addFilter(constraintfl);

					scan.setFilter(fl);

					ResultScanner rs = table.getScanner(scan);

					Result rr;
					while ((rr = rs.next()) != null) {

						long source = Bytes.toLong(rr.getValue(
								Bytes.toBytes("subdue"),
								Bytes.toBytes("source")));
						long target = Bytes.toLong(rr.getValue(
								Bytes.toBytes("subdue"),
								Bytes.toBytes("target")));
						String edge = new String(rr.getValue(
								Bytes.toBytes("subdue"), Bytes.toBytes("edge")));

						String edgeStr = String.format("%s %s \"%s\"", source,
								target, edge);
						//System.out.println(edgeStr);
						edgeSet.add(edgeStr);

					}
					rs.close();

					//context.write(new Text("v"), value);
					
					mos.write(namedOutput, new Text("v"), value);
				}
				for (String edge : edgeSet) {
					//context.write(new Text("e"), new Text(edge));
					mos.write(namedOutput, new Text("e"), new Text(edge));
				}
				mos.close();
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

	public static void run(String dataset, String limit, String output) {
		Configuration config = new Configuration();
		config.set("dataset", dataset);
		// config.set("offset", offset);

		try {

			HTable datasetTable = new HTable(config, dataset);

			Scan countScan = new Scan();

			Filter vertexFilter = new SingleColumnValueFilter(
					Bytes.toBytes("subdue"), Bytes.toBytes("type"),
					CompareFilter.CompareOp.EQUAL, Bytes.toBytes("v"));

			countScan.setFilter(vertexFilter);

			ResultScanner rs = datasetTable.getScanner(countScan);
			long count = 0;
			Result rr;
			while ((rr = rs.next()) != null) {
				count++;
			}
			datasetTable.close();
			long longLimit = Long.parseLong(limit);
			long fixedLimit = longLimit;
			long offset = 1;
			int i;
			for (i = 1; longLimit <= count; i++) {
				config.setLong("limit", longLimit);
				config.set("namedOutput", String.format("%s0%s", dataset.replace(".", ""), i));
				launchJob(dataset, output, config, vertexFilter, longLimit,
						offset, i);
				offset = fixedLimit * i + 1;
				longLimit += fixedLimit;
			}
			longLimit = count;
			config.setLong("limit", longLimit);
						
			config.set("namedOutput", String.format("%s0%s", dataset.replace(".", ""), i));
			launchJob(dataset, output, config, vertexFilter, longLimit, offset,
					i);
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

	private static void launchJob(String dataset, String output,
			Configuration config, Filter vertexFilter, long longLimit,
			long offset, int part) throws IOException, InterruptedException,
			ClassNotFoundException {
		Job job = new Job(config);
		job.setJarByClass(GraphSplitter.class);
		job.setJobName(String.format("[LDClassifier]%s-splitter[%s-%s]",
				dataset, offset, longLimit));
		
		MultipleOutputs.addNamedOutput(job,
				String.format("%s0%s", dataset.replace(".", ""), part),
				TextOutputFormat.class, Text.class, Text.class);

		Scan scan = new Scan();
		List<Filter> filterList = new ArrayList<Filter>();

		Filter offsetFilter = new SingleColumnValueFilter(
				Bytes.toBytes("subdue"), Bytes.toBytes("id"),
				CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(offset));
		Filter limitFilter = new SingleColumnValueFilter(
				Bytes.toBytes("subdue"), Bytes.toBytes("id"),
				CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(longLimit));

		filterList.add(vertexFilter);
		filterList.add(limitFilter);
		filterList.add(offsetFilter);
		FilterList fl = new FilterList(Operator.MUST_PASS_ALL, filterList);
		scan.setFilter(fl);

		TableMapReduceUtil.initTableMapperJob(dataset, scan,
				GraphSplitterMapper.class, ImmutableBytesWritable.class,
				Result.class, job);

		FileOutputFormat.setOutputPath(
				job,
				new Path(String.format("%s/%s/%s-%s", output,
						dataset.replace(".", ""), offset, longLimit)));

		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Text.class);

		job.setReducerClass(GraphSplitterReducer.class);

		job.submit();
	}
}
