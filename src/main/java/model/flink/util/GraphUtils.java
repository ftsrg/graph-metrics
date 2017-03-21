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
import org.apache.log4j.Logger;

public final class GraphUtils {

	final static Logger logger = Logger.getLogger(GraphUtils.class);
	private final static String vertexPath = "resources/vertices.txt";
	private final static String edgePath = "resources/edges.txt";

	public static Graph<IntValue, String, String> createGraph(final ExecutionEnvironment env) {
		List<Vertex<IntValue, String>> vertices = null;
		List<Edge<IntValue, String>> edges = null;
		try {
			vertices = readVertices();
			edges = readEdges();
		} catch (FileNotFoundException exception) {
			logger.error(exception);
		}

		return Graph.fromCollection(vertices, edges, env);
	}

	private static List<Vertex<IntValue, String>> readVertices() throws FileNotFoundException {
		Scanner scanner = new Scanner(new File(vertexPath));
		List<Vertex<IntValue, String>> vertices = new ArrayList<>();
		while (scanner.hasNext()) {
			String[] line = scanner.nextLine().split(" ");
			IntValue id = new IntValue(Integer.parseInt(StringUtils.stripStart(line[0], "0")));
			String label = line[1];
			vertices.add(new Vertex<>(id, label));
		}
		scanner.close();
		return vertices;
	}

	private static List<Edge<IntValue, String>> readEdges() throws FileNotFoundException {
		Scanner scanner = new Scanner(new File(edgePath));
		List<Edge<IntValue, String>> edges = new ArrayList<>();
		while (scanner.hasNext()) {
			String[] line = scanner.nextLine().split(" ");
			IntValue fromVertexId = new IntValue(Integer.parseInt(StringUtils.stripStart(line[0], "0")));
			IntValue toVertexId = new IntValue(Integer.parseInt(StringUtils.stripStart(line[1], "0")));
			String label = line[2];
			edges.add(new Edge<>(fromVertexId, toVertexId, label));
		}
		scanner.close();
		return edges;
	}

	private GraphUtils() {
	}
}
