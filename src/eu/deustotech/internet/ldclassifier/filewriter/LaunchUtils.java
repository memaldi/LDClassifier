package eu.deustotech.internet.ldclassifier.filewriter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class LaunchUtils {
	public static Job launch(String input, String output, String jobName,
			String filter, String dataset, Configuration fileConfig,
			Class<? extends Mapper> mapper, Class<? extends Reducer> reducer)
			throws IOException {

		fileConfig.set("dataset", dataset);
		Job fileJob = new Job(fileConfig);
		fileJob.setJarByClass(VertexWriter.class);
		fileJob.setJobName(String.format("[LDClassifier]%s-%s", dataset,
				jobName));

		// TextInputFormat.addInputPath(fileJob, new Path(input));

		Scan scan = new Scan();

		if (filter != null) {
			List<Filter> filters = new ArrayList<Filter>();
	
			Filter typeFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
					new BinaryComparator(Bytes.toBytes(filter)));
			filters.add(typeFilter);
	
			FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL,
					filters);
	
			scan.setFilter(fl);
		}

		TableMapReduceUtil.initTableMapperJob(dataset, scan,
				(Class<? extends TableMapper>) mapper, ImmutableBytesWritable.class,
				Result.class, fileJob);
		if (output == null) {
			fileJob.setOutputFormatClass(NullOutputFormat.class);
		}

		FileOutputFormat.setOutputPath(fileJob,
				new Path(output + "/" + dataset.replace(".", "")));
		MultipleOutputs.addNamedOutput(fileJob, dataset.replace(".", ""),
				TextOutputFormat.class, Text.class, Text.class);

		fileJob.setMapOutputKeyClass(ImmutableBytesWritable.class);
		fileJob.setMapOutputValueClass(Text.class);
		fileJob.setOutputKeyClass(ImmutableBytesWritable.class);
		fileJob.setOutputValueClass(Text.class);

		fileJob.setMapperClass(mapper);
		if (reducer != null) {
			fileJob.setReducerClass(reducer);
		}

		return fileJob;

	}

	public static Set<String> getDatasets(String input, FileSystem fs)
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
