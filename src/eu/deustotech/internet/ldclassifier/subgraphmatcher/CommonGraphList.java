package eu.deustotech.internet.ldclassifier.subgraphmatcher;

import java.util.ArrayList;

public class CommonGraphList extends ArrayList<CommonGraph> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public CommonGraph searchCommonGraph(Graph graph_i, Graph graph_j) {
		for (CommonGraph cg : this) {
			if (cg.getSource().equals(graph_j) && cg.getTarget().equals(graph_j)) {
				return cg;
			}
		}
		return null;
	}
	
}
