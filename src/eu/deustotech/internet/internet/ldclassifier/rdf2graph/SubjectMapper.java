package eu.deustotech.internet.internet.ldclassifier.rdf2graph;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import com.clarkparsia.stardog.StardogException;
import com.clarkparsia.stardog.api.Connection;
import com.clarkparsia.stardog.api.ConnectionConfiguration;
import com.clarkparsia.stardog.api.Query;

public class SubjectMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) {
		try {
			String dataset = value.toString();
			System.out.println("Extracting subjects from " + dataset);
			
			Configuration config = HBaseConfiguration.create();
			HBaseAdmin hbase = new HBaseAdmin(config);
			try {
				hbase.disableTable(dataset);
				hbase.deleteTable(dataset);
			} catch (Exception e) {
				System.out.println("Database dont exists!");
			}
			
			HTableDescriptor desc = new HTableDescriptor(dataset);
			HColumnDescriptor subdue = new HColumnDescriptor("subdue".getBytes());
			
			desc.addFamily(subdue);
			hbase.createTable(desc);
			
			HTable table = new HTable(config, dataset);
			
			
			Connection aConn = ConnectionConfiguration.to(dataset).url("http://helheim.deusto.es:5822").credentials("admin", "admin").connect();
			Query classQuery = aConn.query("SELECT DISTINCT ?subject ?class WHERE {  ?subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?class }");
			TupleQueryResult classResult = classQuery.executeSelect();
			String subjectClass = null;
			long id = 1;
			while (classResult.hasNext()) {
				BindingSet classSet = classResult.next();
				subjectClass = classSet.getBinding("class").getValue().stringValue();
				
				Put p = new Put(Bytes.toBytes(String.valueOf(id)));
				//p.add(Bytes.toBytes("subdue"), Bytes.toBytes("id"),	Bytes.toBytes(id));
				p.add(Bytes.toBytes("subdue"), Bytes.toBytes("uri"), Bytes.toBytes(subjectClass));
				context.write(new Text(dataset), new Text(id + "," + subjectClass));
				id++;
				table.put(p);
			}
			
			aConn.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (StardogException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
