package eu.deustotech.internet.ldclassifier.splitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GraphSplitter {

	public static class GraphSplitterMapper extends
			TableMapper<ImmutableBytesWritable, Text> {
		@Override
		public void map(ImmutableBytesWritable key, Result value,
				Context context) {

			System.out.println(value.toString());
			
			/*System.out.println(new String(value.getValue(
					Bytes.toBytes("subdue"), Bytes.toBytes("id"))));*/
		}
	}

	public static void run(String dataset, String limit, String output) {
		Configuration config = new Configuration();
		config.set("dataset", dataset);
		try {
			Job job = new Job(config);
			job.setJarByClass(GraphSplitter.class);
			job.setJobName(String.format("[LDClassifier]%s-splitter", dataset));

			Scan scan = new Scan();
			List<Filter> filterList = new ArrayList<Filter>();
			/*
			 * Filter keyFilter = new RowFilter(
			 * CompareFilter.CompareOp.LESS_OR_EQUAL, new
			 * BinaryComparator(Bytes.toBytes(limit)));
			 */
			Filter keyFilter = new SingleColumnValueFilter(
					Bytes.toBytes("subdue"), Bytes.toBytes("id"),
					CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(limit));
			/*
			 * Filter vertexFilter = new ValueFilter(
			 * CompareFilter.CompareOp.EQUAL, new BinaryComparator(
			 * Bytes.toBytes("v")));
			 */
			Filter vertexFilter = new SingleColumnValueFilter(
					Bytes.toBytes("subdue"), Bytes.toBytes("type"),
					CompareFilter.CompareOp.EQUAL, Bytes.toBytes("v"));
			filterList.add(vertexFilter);
			filterList.add(keyFilter);
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
