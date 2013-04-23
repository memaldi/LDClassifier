package eu.deustotech.internet.ldclassifier.main;

import eu.deustotech.internet.ldclassifier.edgegenerator.EdgeGenerator;
import eu.deustotech.internet.ldclassifier.filewriter.EdgeWriter;
import eu.deustotech.internet.ldclassifier.filewriter.VertexWriter;
import eu.deustotech.internet.ldclassifier.loader.TripleLoader;

public class LDClassifier {

	static public void main(String[] args) {

		if (args.length != 3) {
			System.out.println("Invalid arg number!");
			System.out
					.println("Usage: LDClassifier [load | generateEdges | writeVertex | writeEdges] <inputDir> <outputDir> ");
			System.exit(1);
		} else if ("load".equals(args[0])) {
			TripleLoader.run(args[1], args[2]);
		} else if ("generateEdges".equals(args[0])) {
			EdgeGenerator.run(args[1], args[2]);
		} else if ("writeVertex".equals(args[0])) {
			VertexWriter.run(args[1], args[2]);
		} else if ("writeEdges".equals(args[0])) {
			EdgeWriter.run(args[1], args[2]);
		}
	}

}
