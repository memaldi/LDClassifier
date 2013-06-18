package eu.deustotech.internet.ldclassifier.subgraphmatcher;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.semanticweb.owl.align.AlignmentException;
import org.semanticweb.owl.align.Cell;

public class AlignmentList extends ArrayList<Cell> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Cell searchCell(URI onto1, URI onto2) throws AlignmentException {
		for (Cell cell : this) {
			if (cell.getObject1().toString().equals(String.format("<%s>", onto1.toString())) && cell.getObject2().toString().equals(String.format("<%s>", onto2.toString()))) {
				return cell;
			}
		}
		return null;
	}
	
	public List<Cell> searchCells(URI onto1, URI onto2) {
		List<Cell> cellList = new ArrayList<Cell>();
		for (Cell cell : this) {
			if (cell.getObject1().toString().equals(String.format("<%s>", onto1.toString())) && cell.getObject2().toString().equals(String.format("<%s>", onto2.toString()))) {
				cellList.add(cell);
			}
		}
		return cellList;
	}
	
}
