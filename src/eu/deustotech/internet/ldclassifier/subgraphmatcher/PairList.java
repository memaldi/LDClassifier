package eu.deustotech.internet.ldclassifier.subgraphmatcher;

import java.net.URI;
import java.util.ArrayList;

public class PairList extends ArrayList<MatchedPair> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public MatchedPair searchPair(URI source, URI target) {
		for (MatchedPair pair : this) {
			if (pair.getSource().equals(source) && pair.getTarget().equals(target)) {
				return pair;
			}
		}
		return null;
	}
}
