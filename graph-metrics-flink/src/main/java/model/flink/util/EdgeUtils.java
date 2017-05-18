package model.flink.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;

public class EdgeUtils {
	
	final static Logger logger = Logger.getLogger(EdgeUtils.class);
	public static Map<IntValue, List<Edge<IntValue, String>>> edgeMap;
	
	public static void init(Graph<IntValue, NullValue, String> graph) {
		edgeMap = new HashMap<>();
		List<Edge<IntValue, String>> edges = null;
		try {
			edges = graph.getEdges().collect();
			for (Edge<IntValue, String> edge : edges) {
				List<Edge<IntValue, String>> edgeList = null;
				if (edgeMap.containsKey(edge.getSource())) {
					edgeList = edgeMap.get(edge.getSource());
					edgeList.add(edge);
					edgeMap.put(edge.getSource(), edgeList);
				} else {
					edgeList = new ArrayList<>();
					edgeList.add(edge);
					edgeMap.put(edge.getSource(), edgeList);
				}
				if (edgeMap.containsKey(edge.getTarget())) {
					Edge<IntValue, String> reversedEdge = new Edge<>(edge.getTarget(), edge.getSource(), edge.getValue());
					edgeList = edgeMap.get(edge.getTarget());
					edgeList.add(reversedEdge);
					edgeMap.put(edge.getTarget(), edgeList);
				} else {
					Edge<IntValue, String> reversedEdge = new Edge<>(edge.getTarget(), edge.getSource(), edge.getValue());
					edgeList = new ArrayList<>();
					edgeList.add(reversedEdge);
					edgeMap.put(edge.getTarget(), edgeList);
				}
				
			}
		} catch (Exception exception) {
			logger.error(exception);
		}
	}
	
	public static void print() {
		Joiner.MapJoiner mapJoiner = Joiner.on(",").withKeyValueSeparator("=");
		System.out.println(mapJoiner.join(edgeMap));
	}
	
	public static Map<IntValue, List<Edge<IntValue, String>>> getEdgeMap() {
		return edgeMap;
	}
	
	public static List<Edge<IntValue, String>> getEdgeList(IntValue key) {
		if (edgeMap.containsKey(key)) {
			return edgeMap.get(key);
		}
		return null;
	}
}
