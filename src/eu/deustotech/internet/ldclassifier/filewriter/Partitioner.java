package eu.deustotech.internet.ldclassifier.filewriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Partitioner {
	
	public static class PartitionerMapper extends TableMapper<ImmutableBytesWritable, Text> {
		
	}

	public static void run(String dataset, String output, long limit) {
		Configuration config = new Configuration();

		try {
			long i = 1;
			long offset = 0;
			boolean end = false;
			while (!end) {
				limit = limit * i;
				Job job = new Job(config);
				job.setJarByClass(Partitioner.class);

				Scan scan = new Scan();

				List<Filter> filters = new ArrayList<Filter>();
				Filter offsetFilter = new SingleColumnValueFilter(
						Bytes.toBytes("subdue"), Bytes.toBytes("id"),
						CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(offset));
				Filter limitFilter = new SingleColumnValueFilter(
						Bytes.toBytes("subdue"), Bytes.toBytes("id"),
						CompareFilter.CompareOp.LESS, Bytes.toBytes(limit));
				
				filters.add(offsetFilter);
				filters.add(limitFilter);
				
				FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
				scan.setFilter(fl);
			
				TableMapReduceUtil.initTableMapperJob(dataset, scan,
						PartitionerMapper.class, ImmutableBytesWritable.class,
						Result.class, job);
				
				FileOutputFormat.setOutputPath(job,
						new Path(output + "/" + dataset.replace(".", "")));
				
				job.submit();
				
				Map<String, Object> scanMap = scan.toMap();
				
				if (scanMap.isEmpty()) {
					end = true;
				}
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
