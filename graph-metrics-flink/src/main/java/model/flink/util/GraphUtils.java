package model.flink.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;

public final class GraphUtils {

	final static Logger logger = Logger.getLogger(GraphUtils.class);
	private final static String vertexPath = "resources/flink_vertices";
	private final static String edgePath = "resources/flink_edges";

	public static Graph<IntValue, NullValue, String> createGraph(final ExecutionEnvironment env) {
		List<Vertex<IntValue, NullValue>> vertices = null;
		List<Edge<IntValue, String>> edges = null;
		try {
			vertices = readVertices();
			edges = readEdges();
		} catch (FileNotFoundException exception) {
			logger.error(exception);
		}

		return Graph.fromCollection(vertices, edges, env);
	}

	private static List<Vertex<IntValue, NullValue>> readVertices() throws FileNotFoundException {
		Scanner scanner = new Scanner(new File(vertexPath));
		List<Vertex<IntValue, NullValue>> vertices = new ArrayList<>();
		while (scanner.hasNext()) {
			String line = scanner.nextLine();
			IntValue id = new IntValue(Integer.parseInt(line));
			vertices.add(new Vertex<>(id, NullValue.getInstance()));
		}
		scanner.close();
		return vertices;
	}

	private static List<Edge<IntValue, String>> readEdges() throws FileNotFoundException {
		Scanner scanner = new Scanner(new File(edgePath));
		List<Edge<IntValue, String>> edges = new ArrayList<>();
		while (scanner.hasNext()) {
			String[] line = scanner.nextLine().split(" ");
			IntValue fromVertexId = new IntValue(Integer.parseInt(line[0]));
			IntValue toVertexId = new IntValue(Integer.parseInt(line[1]));
			String label = line[2];
			edges.add(new Edge<>(fromVertexId, toVertexId, label));
		}
		scanner.close();
		return edges;
	}

	private GraphUtils() {
	}
}
