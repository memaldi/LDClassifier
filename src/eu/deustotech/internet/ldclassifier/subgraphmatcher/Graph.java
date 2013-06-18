package eu.deustotech.internet.ldclassifier.subgraphmatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Graph {
	
	private UUID id;
	private List<Vertex> vertexList;
	private List<Edge> edgeList;
	private String name;
	
	public Graph() {
		this.id = UUID.randomUUID();
		this.vertexList = new ArrayList<Vertex>();
		this.edgeList = new ArrayList<Edge>();
	}
	
	public Graph(Graph graph) {
		this.id = UUID.randomUUID();
		this.vertexList = new ArrayList<Vertex>();
		this.vertexList.addAll(graph.getVertexList());
		this.edgeList = new ArrayList<Edge>();
		this.edgeList.addAll(graph.getEdgeList());
		this.name = graph.getName();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void replace(String source, String target) {
		for (Vertex vertex : this.vertexList) {
			if (vertex.getLabel().equals(source)) {
				int index = this.vertexList.indexOf(vertex);
				//vertex.setLabel(target);
				this.vertexList.set(index, new Vertex(vertex.getId(), target));
				//this.vertexList.set(index, new Vertex(new Long(1), "a"));
			}
		}
		
		for (Edge edge : this.edgeList) {
			if (edge.getLabel().equals(source)) {
				int index = this.edgeList.indexOf(edge);
				edge.setLabel(target);
				this.edgeList.set(index, edge);
			}
		}
	}
	
	public boolean equals(Graph graph) {
		if (graph == null)
			return false;
		if (this.id.compareTo(graph.getId()) == 0) {
			return true;
		} else {
			return false;
		}
	}
	
	public void addVertex(Vertex vertex) {
		this.vertexList.add(vertex);
	}
	
	public void addEdge(Edge edge) {
		this.edgeList.add(edge);
	}
	
	public UUID getId() {
		return id;
	}

	public List<Vertex> getVertexList() {
		return vertexList;
	}

	public List<Edge> getEdgeList() {
		return edgeList;
	}

	public static class Vertex {
		private Long id;
		private String label;
		
		public Vertex(Long id, String label) {
			this.id = id;
			this.label = label;
		}

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getLabel() {
			return label;
		}

		public void setLabel(String label) {
			this.label = label;
		}
		
	}
	
	public static class Edge {
		private UUID id;
		private Long source;
		private Long target;
		private String label;
		
		public Edge(Long source, Long target, String label) {
			//this.id = id;
			this.id = UUID.randomUUID();
			this.source = source;
			this.target = target;
			this.label = label;
		}

		public UUID getId() {
			return id;
		}

		public Long getSource() {
			return source;
		}

		public void setSource(Long source) {
			this.source = source;
		}

		public Long getTarget() {
			return target;
		}

		public void setTarget(Long target) {
			this.target = target;
		}

		public String getLabel() {
			return label;
		}

		public void setLabel(String label) {
			this.label = label;
		}
		
	}
}
