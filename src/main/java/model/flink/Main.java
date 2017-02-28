package model.flink;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.clustering.directed.TriangleListing;
import org.apache.flink.graph.library.clustering.directed.TriangleListing.Result;
import org.apache.flink.types.IntValue;

public class Main {
	public static void main(String[] args) throws Exception {
		//BasicConfigurator.configure();
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Create new vertices with a Integer ID and a String value.
		Vertex<IntValue, String> v1 = new Vertex<>(new IntValue(1), "R1");
		Vertex<IntValue, String> v2 = new Vertex<>(new IntValue(2), "E1");
		Vertex<IntValue, String> v3 = new Vertex<>(new IntValue(3), "Off");
		Vertex<IntValue, String> v4 = new Vertex<>(new IntValue(4), "On");
		Vertex<IntValue, String> v5 = new Vertex<>(new IntValue(5), "T1");
		Vertex<IntValue, String> v6 = new Vertex<>(new IntValue(6), "T2");
		Vertex<IntValue, String> v7 = new Vertex<>(new IntValue(7), "T3");
		Vertex<IntValue, String> v8 = new Vertex<>(new IntValue(8), "R2");
		Vertex<IntValue, String> v9 = new Vertex<>(new IntValue(9), "E2");
		Vertex<IntValue, String> v10 = new Vertex<>(new IntValue(10), "L");
		Vertex<IntValue, String> v11 = new Vertex<>(new IntValue(11), "H");
		Vertex<IntValue, String> v12 = new Vertex<>(new IntValue(12), "T4");
		Vertex<IntValue, String> v13 = new Vertex<>(new IntValue(13), "T5");
		Vertex<IntValue, String> v14 = new Vertex<>(new IntValue(14), "T6");


		Edge<IntValue, String> e01 = new Edge<>(new IntValue(1), new IntValue(2), DimensionType.VERTICES.getLabel());
		Edge<IntValue, String> e02 = new Edge<>(new IntValue(1), new IntValue(3), DimensionType.VERTICES.getLabel());
		Edge<IntValue, String> e03 = new Edge<>(new IntValue(1), new IntValue(4), DimensionType.VERTICES.getLabel());
		Edge<IntValue, String> e04 = new Edge<>(new IntValue(8), new IntValue(9), DimensionType.VERTICES.getLabel());
		Edge<IntValue, String> e05 = new Edge<>(new IntValue(8), new IntValue(10), DimensionType.VERTICES.getLabel());
		Edge<IntValue, String> e06 = new Edge<>(new IntValue(8), new IntValue(11), DimensionType.VERTICES.getLabel());
		Edge<IntValue, String> e07 = new Edge<>(new IntValue(5), new IntValue(3), DimensionType.TARGET.getLabel());
		Edge<IntValue, String> e08 = new Edge<>(new IntValue(7), new IntValue(3), DimensionType.TARGET.getLabel());
		Edge<IntValue, String> e09 = new Edge<>(new IntValue(6), new IntValue(4), DimensionType.TARGET.getLabel());
		Edge<IntValue, String> e10 = new Edge<>(new IntValue(12), new IntValue(10), DimensionType.TARGET.getLabel());
		Edge<IntValue, String> e11 = new Edge<>(new IntValue(14), new IntValue(10), DimensionType.TARGET.getLabel());
		Edge<IntValue, String> e12 = new Edge<>(new IntValue(13), new IntValue(11), DimensionType.TARGET.getLabel());
		Edge<IntValue, String> e13 = new Edge<>(new IntValue(3), new IntValue(5), DimensionType.INCOMING.getLabel());
		Edge<IntValue, String> e14 = new Edge<>(new IntValue(3), new IntValue(7), DimensionType.INCOMING.getLabel());
		Edge<IntValue, String> e15 = new Edge<>(new IntValue(4), new IntValue(6), DimensionType.INCOMING.getLabel());
		Edge<IntValue, String> e16 = new Edge<>(new IntValue(10), new IntValue(12), DimensionType.INCOMING.getLabel());
		Edge<IntValue, String> e17 = new Edge<>(new IntValue(10), new IntValue(14), DimensionType.INCOMING.getLabel());
		Edge<IntValue, String> e18 = new Edge<>(new IntValue(11), new IntValue(13), DimensionType.INCOMING.getLabel());
		Edge<IntValue, String> e19 = new Edge<>(new IntValue(2), new IntValue(5), DimensionType.OUTGOING.getLabel());
		Edge<IntValue, String> e20 = new Edge<>(new IntValue(3), new IntValue(6), DimensionType.OUTGOING.getLabel());
		Edge<IntValue, String> e21 = new Edge<>(new IntValue(4), new IntValue(7), DimensionType.OUTGOING.getLabel());
		Edge<IntValue, String> e22 = new Edge<>(new IntValue(9), new IntValue(12), DimensionType.OUTGOING.getLabel());
		Edge<IntValue, String> e23 = new Edge<>(new IntValue(10), new IntValue(13), DimensionType.OUTGOING.getLabel());
		Edge<IntValue, String> e24 = new Edge<>(new IntValue(11), new IntValue(14), DimensionType.OUTGOING.getLabel());
		Edge<IntValue, String> e25 = new Edge<>(new IntValue(4), new IntValue(8), DimensionType.REGIONS.getLabel());
		Edge<IntValue, String> e26 = new Edge<>(new IntValue(2), new IntValue(3), DimensionType.REGIONS.getLabel());

		// Create graph.
		List<Vertex<IntValue, String>> vertices = new ArrayList<>();

		vertices.add(v1);
		vertices.add(v2);
		vertices.add(v3);
		vertices.add(v4);
		vertices.add(v5);
		vertices.add(v6);
		vertices.add(v7);
		vertices.add(v8);
		vertices.add(v9);
		vertices.add(v10);
		vertices.add(v11);
		vertices.add(v12);
		vertices.add(v13);
		vertices.add(v14);

		List<Edge<IntValue, String>> edges = new ArrayList<>();

		edges.add(e01);
		edges.add(e02);
		edges.add(e03);
		edges.add(e04);
		edges.add(e05);
		edges.add(e06);
		edges.add(e07);
		edges.add(e08);
		edges.add(e09);
		edges.add(e10);
		edges.add(e11);
		edges.add(e12);
		edges.add(e13);
		edges.add(e14);
		edges.add(e15);
		edges.add(e16);
		edges.add(e17);
		edges.add(e18);
		edges.add(e19);
		edges.add(e20);
		edges.add(e21);
		edges.add(e22);
		edges.add(e23);
		edges.add(e24);
		edges.add(e25);
		edges.add(e26);

		Graph<IntValue, String, String> graph = Graph.fromCollection(vertices, edges, env);

////		DataSet<Tuple2<IntValue, LongValue>> inDegrees = graph.inDegrees();
////		DataSet<Tuple2<IntValue, LongValue>> outDegrees = graph.outDegrees();
////		System.out.println("in-degrees:");
////		inDegrees.print();
////		System.out.println("out-degrees:");
////		outDegrees.print();
//
//		// System.out.println("Average Degree: " + (double)
//		// (graph.numberOfEdges() * 2 / graph.numberOfVertices()));
//
////		LocalClusteringCoefficient<IntValue, String, String> algo = new LocalClusteringCoefficient<>();
////		DataSet<Result<IntValue>> result = graph.run(algo);
////		result.print();
		
		
		Metrics metrics = new Metrics(graph);
//		metrics.dimensionalDegree(null, Stream.of(DimensionType.OUTGOING, DimensionType.TARGET, DimensionType.INCOMING).collect(Collectors.toList()));
//		metrics.nodeDimensionActivity();
//		metrics.nodeDimensionConnectivity();
//		metrics.nodeExclusiveDimensionConnectivity();
//		metrics.edgeDimensionActivity();
//		metrics.edgeDimensionConnectivity();
//		metrics.nodeActivity(8);
//		metrics.multiplexParticipationCoefficient(1);
		TriangleListing<IntValue, String, String> triangleListing = new TriangleListing<>();
		DataSet<Result<IntValue>> ds = graph.run(triangleListing);
		ds.print();
		
		
		
	}

	

}
