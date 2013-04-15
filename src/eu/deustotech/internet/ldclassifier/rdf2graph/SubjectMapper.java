package eu.deustotech.internet.ldclassifier.rdf2graph;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
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
			HTable table = getTable(dataset, config);

			Connection aConn = ConnectionConfiguration.to(dataset)
					.url("http://helheim.deusto.es:5822")
					.credentials("admin", "admin").connect();

			generateNodes(context, dataset, table, aConn);

			
			
			/*
			 * Scan s = new Scan(); s.addColumn(Bytes.toBytes("nodes"),
			 * Bytes.toBytes("uri")); ResultScanner scanner =
			 * table.getScanner(s);
			 * 
			 * String subject = null;
			 * 
			 * for (Result rr = scanner.next(); rr != null; rr = scanner.next())
			 * { // System.out.println(new //
			 * String(rr.getValue(Bytes.toBytes("nodes"), //
			 * Bytes.toBytes("class")))); // System.out.println(new
			 * String(rr.getRow())); subject = new
			 * String(rr.getValue(Bytes.toBytes("nodes"),
			 * Bytes.toBytes("class"))); Query objectQuery = aConn .query(String
			 * .format(
			 * "SELECT ?object ?property WHERE { <%s> ?property ?object . FILTER (isIRI(?object) && ?property != <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>) }"
			 * , subject)); TupleQueryResult objectResult =
			 * objectQuery.executeSelect(); while (objectResult.hasNext()) {
			 * 
			 * } }
			 */

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

	private HTable getTable(String dataset, Configuration config)
			throws MasterNotRunningException, ZooKeeperConnectionException,
			IOException {
		HBaseAdmin hbase = new HBaseAdmin(config);
		try {
			hbase.disableTable(dataset);
			hbase.deleteTable(dataset);
		} catch (Exception e) {
			System.out.println("Database dont exists!");
		}

		HTableDescriptor desc = new HTableDescriptor(dataset);
		HColumnDescriptor subdue = new HColumnDescriptor("nodes".getBytes());

		desc.addFamily(subdue);
		hbase.createTable(desc);

		HTable table = new HTable(config, dataset);
		return table;
	}

	private void generateNodes(Context context, String dataset, HTable table,
			Connection aConn) throws StardogException,
			QueryEvaluationException, IOException, InterruptedException {
		Query classQuery = aConn
				.query("SELECT DISTINCT ?subject ?class WHERE {  ?subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?class }");
		TupleQueryResult classResult = classQuery.executeSelect();
		String subjectClass = null;
		String subject = null;
		long id = 1;
		while (classResult.hasNext()) {
			BindingSet classSet = classResult.next();
			subjectClass = classSet.getBinding("class").getValue()
					.stringValue();
			subject = classSet.getBinding("subject").getValue().stringValue();

			Put p = new Put(Bytes.toBytes(String.valueOf(id)));
			p.add(Bytes.toBytes("nodes"), Bytes.toBytes("class"),
					Bytes.toBytes(subjectClass));
			p.add(Bytes.toBytes("nodes"), Bytes.toBytes("subject"),
					Bytes.toBytes(subject));
			context.write(new Text(String.valueOf(id)), new Text(subject));
			id++;

			table.put(p);
		}

		classResult.close();
	}
}
