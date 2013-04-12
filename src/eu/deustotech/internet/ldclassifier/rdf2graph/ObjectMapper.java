package eu.deustotech.internet.ldclassifier.rdf2graph;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import com.clarkparsia.stardog.StardogException;
import com.clarkparsia.stardog.api.Connection;
import com.clarkparsia.stardog.api.ConnectionConfiguration;
import com.clarkparsia.stardog.api.Query;

public class ObjectMapper extends TableMapper<Text, Text> {

	@Override
	public void map(ImmutableBytesWritable row, Result values, Context context)
			throws IOException {
		System.out.println("Mapper!");
		Configuration conf = context.getConfiguration();
		String subject = new String(values.getValue(Bytes.toBytes("nodes"),
				Bytes.toBytes("subject")));

		try {
			Connection aConn = ConnectionConfiguration.to(conf.get("dataset"))
					.url("http://helheim.deusto.es:5822")
					.credentials("admin", "admin").connect();
			Query objectQuery = aConn
					.query(String
							.format("SELECT ?object ?property WHERE { <%s> ?property ?object . FILTER (isIRI(?object) && ?property != <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>) }",
									subject));
			TupleQueryResult result = objectQuery.executeSelect();

			String object = null;
			String property = null;
			
			while(result.hasNext()) {
				BindingSet classSet = result.next();
				object = classSet.getBinding("object").getValue()
						.stringValue();
				property = classSet.getBinding("property").getValue()
						.stringValue();
				context.write(new Text(object), new Text(object));
			}
			
			
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

		/*
		 * for (KeyValue value : values.list()) { System.out.println(new
		 * String(value.getValue())); }
		 */
	}
}
