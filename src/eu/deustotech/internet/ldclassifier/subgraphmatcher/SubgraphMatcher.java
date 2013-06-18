package eu.deustotech.internet.ldclassifier.subgraphmatcher;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.semanticweb.owl.align.AlignmentException;
import org.semanticweb.owl.align.AlignmentProcess;
import org.semanticweb.owl.align.Cell;

import eu.deustotech.internet.ldclassifier.subgraphmatcher.Graph.Edge;
import eu.deustotech.internet.ldclassifier.subgraphmatcher.Graph.Vertex;
import fr.inrialpes.exmo.align.impl.BasicParameters;
import fr.inrialpes.exmo.align.impl.method.NameAndPropertyAlignment;

public class SubgraphMatcher {

	private static List<URI> ontoList = new ArrayList<URI>();
	private static List<String> alignClassList = new ArrayList<String>();
	private static int alignmentCount = 3;
	private static float threshold = (float) 0.5;

	static {
		alignClassList
				.add("fr.inrialpes.exmo.align.impl.method.NameAndPropertyAlignment");
		alignClassList
				.add("fr.inrialpes.exmo.align.impl.method.ClassStructAlignment");
		alignClassList
				.add("fr.inrialpes.exmo.align.impl.method.StringDistAlignment");
		//alignClassList
		//		.add("fr.inrialpes.exmo.align.impl.method.StrucSubsDistAlignment");
	}

	public static void main(String[] args) {
		run("/home/mikel/doctorado/subgraphs/subdue");
	}

	public static void run(String graphDir) {

		List<Graph> graphList = getGraphs(graphDir);
		//AlignmentList alignmentList = new AlignmentList();
		//Map<Cell, List<Double>> cellMap = new HashMap<Cell, List<Double>>();
		PairList pairList = new PairList();
		
		for (URI onto_i : ontoList) {
			for (URI onto_j : ontoList) {
				if (!onto_i.equals(onto_j)) {

					// AlignmentProcess aProcess = new
					// NameAndPropertyAlignment();
					for (String className : alignClassList) {
						try {
							AlignmentProcess aProcess = (AlignmentProcess) Class
									.forName(className).newInstance();
							Properties params = new BasicParameters();
							aProcess.init(onto_i, onto_j);
							aProcess.align(null, params);

							Enumeration<Cell> cells = aProcess.getElements();

							while (cells.hasMoreElements()) {
								Cell cell = cells.nextElement();
								// System.out.println(String.format("%s - %s - %s",
								// cell.getObject1(), cell.getObject2(),
								// cell.getStrength()));
								//System.out.println(String.format("%s - %s - %s", cell.getObject1(),
								//		cell.getObject2(), cell.getStrength()));
								
								MatchedPair mp;
								
								if ((mp = pairList.searchPair(new URI(cell.getObject1().toString().replace("<", "").replace(">", "")), new URI(cell.getObject2().toString().replace("<", "").replace(">", "")))) == null) {
									mp = new MatchedPair(new URI(cell.getObject1().toString().replace("<", "").replace(">", "")), new URI(cell.getObject2().toString().replace("<", "").replace(">", "")));
								}
								mp.addAlignment(className, cell.getStrength());
								pairList.add(mp);
							}

						} catch (AlignmentException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (InstantiationException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (IllegalAccessException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (ClassNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (URISyntaxException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
		}

		// System.out.println(alignmentList);
		Set<MatchedPair> matchedSet = new HashSet<MatchedPair>();
		CommonGraphList commonGraphList = new CommonGraphList();
		
		for (Graph graph_i : graphList) {
			for (Graph graph_j : graphList) {
				if (graph_i != graph_j) {
					// System.out.println(String.format("%s - %s",
					// graph_i.getId(), graph_j.getId()));

					List<Vertex> graph_i_vertexList = graph_i.getVertexList();
					List<Vertex> graph_j_vertexList = graph_j.getVertexList();

					for (Vertex vertex_i : graph_i_vertexList) {
						for (Vertex vertex_j : graph_j_vertexList) {
							try {
								MatchedPair mp;
								
								if ((mp = pairList.searchPair(new URI(vertex_i.getLabel()), new URI(vertex_j.getLabel()))) != null) {
									//System.out.println(String.format("%s - %s - %s", mp.getSource(), mp.getTarget(), mp.getAlignmentMap()));
									matchedSet.add(mp);
									float mean = 0;
									for (String alignmentClass : mp.getAlignmentMap().keySet()) {
										mean += mp.getAlignmentMap().get(alignmentClass);
									}
									mean = mean / alignmentCount;
									if (mean > threshold) {
										CommonGraph cg;
										Graph commonGraph_i;
										Graph commonGraph_j;
										if ((cg = commonGraphList.searchCommonGraph(graph_i, graph_j)) == null) {
											cg = new CommonGraph(graph_i, graph_j);
											commonGraphList.add(cg);
											commonGraph_i = new Graph(graph_i);
											commonGraph_j = new Graph(graph_j);
										} else {
											commonGraph_i = cg.getSourceCommon();
											commonGraph_j = cg.getTargetCommon();
										}
										int index = commonGraphList.indexOf(cg);
										
										UUID uuid = UUID.randomUUID();
										
										commonGraph_i.replace(mp.getSource().toString(), uuid.toString());
										commonGraph_j.replace(mp.getTarget().toString(), uuid.toString());
										cg.setSourceCommon(commonGraph_i);
										cg.setTargetCommon(commonGraph_j);
	
										commonGraphList.set(index, cg);
									}
																		
								}
							} catch (URISyntaxException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}

				}
			}
		}

		// System.out.println(cellSet);
		for (MatchedPair mp : matchedSet) {
			System.out.println(String.format("%s - %s - %s", mp.getSource(),
					mp.getTarget(), mp.getAlignmentMap()));
		}
		
		for (CommonGraph cg : commonGraphList) {
			System.out.println("Matched graphs!");
			printCommonGraphs(cg);
		}
	}

	private static void printCommonGraphs(CommonGraph cg) {
		Graph source = cg.getSource();
		System.out.println("Source");
		for (Vertex vertex : source.getVertexList()) {
			System.out.println(String.format("%s", vertex.getLabel()));
		}
		for (Edge edge : source.getEdgeList()) {
			System.out.println(String.format("%s %s %s", edge.getSource(), edge.getTarget(), edge.getLabel()));
		}
		
		Graph target = cg.getTarget();
		System.out.println("Target");
		for (Vertex vertex : target.getVertexList()) {
			System.out.println(String.format("%s", vertex.getLabel()));
		}
		for (Edge edge : target.getEdgeList()) {
			System.out.println(String.format("%s %s %s", edge.getSource(), edge.getTarget(), edge.getLabel()));
		}
		
		Graph sourceCommon = cg.getSourceCommon();
		System.out.println("Source Common");
		for (Vertex vertex : sourceCommon.getVertexList()) {
			System.out.println(String.format("%s", vertex.getLabel()));
		}
		for (Edge edge : sourceCommon.getEdgeList()) {
			System.out.println(String.format("%s %s %s", edge.getSource(), edge.getTarget(), edge.getLabel()));
		}
		
		Graph targetCommon = cg.getTargetCommon();
		System.out.println("Target Common");
		for (Vertex vertex : targetCommon.getVertexList()) {
			System.out.println(String.format("%s", vertex.getLabel()));
		}
		for (Edge edge : targetCommon.getEdgeList()) {
			System.out.println(String.format("%s %s %s", edge.getSource(), edge.getTarget(), edge.getLabel()));
		}
	}

	private static List<Graph> getGraphs(String graphDir) {
		List<Graph> graphList = new ArrayList<Graph>();
		File[] files = new File(graphDir).listFiles();

		for (File file : files) {
			if (!file.isDirectory()) {
				try {
					FileInputStream fstream = new FileInputStream(graphDir
							+ "/" + file.getName());
					DataInputStream in = new DataInputStream(fstream);
					BufferedReader br = new BufferedReader(
							new InputStreamReader(in));
					String strLine;

					Graph graph = new Graph();

					while ((strLine = br.readLine()) != null) {
						String[] strArray = strLine.split(" ");
						if (strArray[0].equals("v")) {
							Vertex vertex = new Vertex(
									Long.parseLong(strArray[1]), strArray[2]);
							URI ontoURI = new URI(getPrefix(vertex.getLabel()));
							if (!ontoList.contains(ontoURI)) {
								ontoList.add(ontoURI);
							}
							graph.addVertex(vertex);
						} else if (strArray[0].equals("d")
								|| strArray[0].equals("e")) {
							Edge edge = new Edge(Long.parseLong(strArray[1]),
									Long.parseLong(strArray[2]), strArray[3]);
							graph.addEdge(edge);
						}
					}

					graphList.add(graph);

					br.close();
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (URISyntaxException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return graphList;
	}

	private static String getPrefix(String URI) {
		if (URI.contains("#")) {
			return URI.split("#")[0] + "#";
		} else {
			String[] splitURI = URI.split("/");
			String prefixURI = "";
			for (int i = 0; i < splitURI.length - 1; i++) {
				prefixURI += splitURI[i] + "/";
			}
			return prefixURI;
		}
	}
}
