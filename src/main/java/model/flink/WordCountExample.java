package model.flink;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexInDegree;
import org.apache.flink.graph.library.clustering.directed.LocalClusteringCoefficient;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;


public class WordCountExample {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
     
        // Create new vertices with a Integer ID and a String value.
        Vertex<Integer, String> v1 = new Vertex<Integer, String>(1, "R1");
        Vertex<Integer, String> v2 = new Vertex<Integer, String>(2, "E1");
        Vertex<Integer, String> v3 = new Vertex<Integer, String>(3, "Off");
        Vertex<Integer, String> v4 = new Vertex<Integer, String>(4, "On");
        Vertex<Integer, String> v5 = new Vertex<Integer, String>(5, "T1");
        Vertex<Integer, String> v6 = new Vertex<Integer, String>(6, "T2");
        Vertex<Integer, String> v7 = new Vertex<Integer, String>(7, "T3");
        Vertex<Integer, String> v8 = new Vertex<Integer, String>(8, "R2");
        Vertex<Integer, String> v9 = new Vertex<Integer, String>(9, "E2");
        Vertex<Integer, String> v10 = new Vertex<Integer, String>(10, "L");
        Vertex<Integer, String> v11 = new Vertex<Integer, String>(11, "H");
        Vertex<Integer, String> v12 = new Vertex<Integer, String>(12, "T4");
        Vertex<Integer, String> v13 = new Vertex<Integer, String>(13, "T5");
        Vertex<Integer, String> v14 = new Vertex<Integer, String>(14, "T6");
        
        // vertices : 1
        Edge<Integer, Integer> e1 = new Edge<Integer, Integer>(1, 2, 1);
        Edge<Integer, Integer> e2 = new Edge<Integer, Integer>(1, 3, 1);
        Edge<Integer, Integer> e3 = new Edge<Integer, Integer>(1, 4, 1);
        Edge<Integer, Integer> e4 = new Edge<Integer, Integer>(8, 9, 1);
        Edge<Integer, Integer> e5 = new Edge<Integer, Integer>(8, 10, 1);
        Edge<Integer, Integer> e6 = new Edge<Integer, Integer>(8, 11, 1);
        
        // target: 2
        Edge<Integer, Integer> e7 = new Edge<Integer, Integer>(5, 3, 2);
        Edge<Integer, Integer> e8 = new Edge<Integer, Integer>(7, 3, 2);
        Edge<Integer, Integer> e9 = new Edge<Integer, Integer>(6, 4, 2);
        Edge<Integer, Integer> e10 = new Edge<Integer, Integer>(12, 10, 2);
        Edge<Integer, Integer> e11 = new Edge<Integer, Integer>(14, 10, 2);
        Edge<Integer, Integer> e12 = new Edge<Integer, Integer>(13, 11, 2);
        
        // incoming: 3
        Edge<Integer, Integer> e13 = new Edge<Integer, Integer>(3, 5, 3);
        Edge<Integer, Integer> e14 = new Edge<Integer, Integer>(3, 7, 3);
        Edge<Integer, Integer> e15 = new Edge<Integer, Integer>(4, 6, 3);
        Edge<Integer, Integer> e16 = new Edge<Integer, Integer>(10, 12, 3);
        Edge<Integer, Integer> e17 = new Edge<Integer, Integer>(10, 14, 3);
        Edge<Integer, Integer> e18 = new Edge<Integer, Integer>(11, 13, 3);
        
        // outgoing: 4
        Edge<Integer, Integer> e19 = new Edge<Integer, Integer>(2, 5, 4);
        Edge<Integer, Integer> e20 = new Edge<Integer, Integer>(3, 6, 4);
        Edge<Integer, Integer> e21 = new Edge<Integer, Integer>(4, 7, 4);
        Edge<Integer, Integer> e22 = new Edge<Integer, Integer>(9, 12, 4);
        Edge<Integer, Integer> e23 = new Edge<Integer, Integer>(10, 13, 4);
        Edge<Integer, Integer> e24 = new Edge<Integer, Integer>(11, 14, 4);
        
        // regions: 5
        Edge<Integer, Integer> e25 = new Edge<Integer, Integer>(4, 8, 5);
        
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
        
        List<Edge<Integer, Integer>> edges = new ArrayList<Edge<Integer,Integer>>();
        
        edges.add(e1);
        edges.add(e2);
        edges.add(e3);
        edges.add(e4);
        edges.add(e5);
        edges.add(e6);
        edges.add(e7);
        edges.add(e8);
        edges.add(e9);
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

        
        Graph<Integer, String, Integer> graph = Graph.fromCollection(vertices, edges, env);
        
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
