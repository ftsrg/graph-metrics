package model.flink;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.LongValue;


public class GraphMetricsExample {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Create new vertices with a Integer ID and a String value.
        Vertex<Integer, String> v1 = new Vertex<>(1, "R1");
        Vertex<Integer, String> v2 = new Vertex<>(2, "E1");
        Vertex<Integer, String> v3 = new Vertex<>(3, "Off");
        Vertex<Integer, String> v4 = new Vertex<>(4, "On");
        Vertex<Integer, String> v5 = new Vertex<>(5, "T1");
        Vertex<Integer, String> v6 = new Vertex<>(6, "T2");
        Vertex<Integer, String> v7 = new Vertex<>(7, "T3");
        Vertex<Integer, String> v8 = new Vertex<>(8, "R2");
        Vertex<Integer, String> v9 = new Vertex<>(9, "E2");
        Vertex<Integer, String> v10 = new Vertex<>(10, "L");
        Vertex<Integer, String> v11 = new Vertex<>(11, "H");
        Vertex<Integer, String> v12 = new Vertex<>(12, "T4");
        Vertex<Integer, String> v13 = new Vertex<>(13, "T5");
        Vertex<Integer, String> v14 = new Vertex<>(14, "T6");

        final String VERTICES = "vertices";
        final String TARGET = "target";
        final String INCOMING = "incoming";
        final String OUTGOING = "outgoing";
        final String REGIONS = "regions";

        // vertices : 1
        Edge<Integer, String> e01 = new Edge<>(1, 2, VERTICES);
        Edge<Integer, String> e02 = new Edge<>(1, 3, VERTICES);
        Edge<Integer, String> e03 = new Edge<>(1, 4, VERTICES);
        Edge<Integer, String> e04 = new Edge<>(8, 9, VERTICES);
        Edge<Integer, String> e05 = new Edge<>(8, 10, VERTICES);
        Edge<Integer, String> e06 = new Edge<>(8, 11, VERTICES);

        // target: 2
        Edge<Integer, String> e07 = new Edge<>(5, 3, TARGET);
        Edge<Integer, String> e08 = new Edge<>(7, 3, TARGET);
        Edge<Integer, String> e09 = new Edge<>(6, 4, TARGET);
        Edge<Integer, String> e10 = new Edge<>(12, 10, TARGET);
        Edge<Integer, String> e11 = new Edge<>(14, 10, TARGET);
        Edge<Integer, String> e12 = new Edge<>(13, 11, TARGET);

        // incoming: 3
        Edge<Integer, String> e13 = new Edge<>(3, 5, INCOMING);
        Edge<Integer, String> e14 = new Edge<>(3, 7, INCOMING);
        Edge<Integer, String> e15 = new Edge<>(4, 6, INCOMING);
        Edge<Integer, String> e16 = new Edge<>(10, 12, INCOMING);
        Edge<Integer, String> e17 = new Edge<>(10, 14, INCOMING);
        Edge<Integer, String> e18 = new Edge<>(11, 13, INCOMING);

        // outgoing: 4
        Edge<Integer, String> e19 = new Edge<>(2, 5, OUTGOING);
        Edge<Integer, String> e20 = new Edge<>(3, 6, OUTGOING);
        Edge<Integer, String> e21 = new Edge<>(4, 7, OUTGOING);
        Edge<Integer, String> e22 = new Edge<>(9, 12, OUTGOING);
        Edge<Integer, String> e23 = new Edge<>(10, 13, OUTGOING);
        Edge<Integer, String> e24 = new Edge<>(11, 14, OUTGOING);

        // regions: 5
        Edge<Integer, String> e25 = new Edge<>(4, 8, REGIONS);

        // Create graph.
        List<Vertex<Integer, String>> vertices = new ArrayList<Vertex<Integer, String>>();

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

        List<Edge<Integer, String>> edges = new ArrayList<>();

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


        Graph<Integer, String, String> graph = Graph.fromCollection(vertices, edges, env);

        DataSet<Tuple2<Integer, LongValue>> inDegrees = graph.inDegrees();
        DataSet<Tuple2<Integer, LongValue>> outDegrees = graph.outDegrees();
        System.out.println("in-degrees:");
        inDegrees.print();
        System.out.println("out-degrees:");
        outDegrees.print();

        System.out.println("Average Degree: " + (double) (graph.numberOfEdges() * 2 / graph.numberOfVertices()));

//        graph.run(new org.apache.flink.graph.library.clustering.directed.LocalClusteringCoefficient<Integer, Integer, Integer>()
//			.setLittleParallelism(0));

    }
}
