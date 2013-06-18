package eu.deustotech.internet.ldclassifier.subgraphmatcher;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MatchedPair {

	private URI source;
	private URI target;
	
	private Map<String, Double> alignmentMap;
	
	public MatchedPair(URI source, URI target) {
		this.source = source;
		this.target = target;
		this.alignmentMap = new HashMap<String, Double>();
	}
	
	public Map<String, Double> getAlignmentMap() {
		return this.alignmentMap;
	}
	
	public void addAlignment(String alignment, Double strength) {
		this.alignmentMap.put(alignment, strength);
	}
	
	public Double getStrength(String alignment) {
		return this.alignmentMap.get(alignment);
	}

	public URI getSource() {
		return source;
	}

	public void setSource(URI source) {
		this.source = source;
	}

	public URI getTarget() {
		return target;
	}

	public void setTarget(URI target) {
		this.target = target;
	}
	
}
