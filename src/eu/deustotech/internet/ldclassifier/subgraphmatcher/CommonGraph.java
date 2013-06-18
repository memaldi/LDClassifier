package eu.deustotech.internet.ldclassifier.subgraphmatcher;

public class CommonGraph {
	private Graph source;
	private Graph target;
	private Graph sourceCommon;
	private Graph targetCommon;
	
	public CommonGraph(Graph source, Graph target) {
		this.source = new Graph(source);
		this.target = new Graph(target);
	}

	public Graph getSource() {
		return source;
	}

	public void setSource(Graph source) {
		this.source = source;
	}

	public Graph getTarget() {
		return target;
	}

	public void setTarget(Graph target) {
		this.target = target;
	}

	public Graph getSourceCommon() {
		return sourceCommon;
	}

	public void setSourceCommon(Graph sourceCommon) {
		this.sourceCommon = sourceCommon;
	}

	public Graph getTargetCommon() {
		return targetCommon;
	}

	public void setTargetCommon(Graph targetCommon) {
		this.targetCommon = targetCommon;
	}
	
}
