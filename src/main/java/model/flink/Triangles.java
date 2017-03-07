package model.flink;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.degree.annotate.undirected.EdgeDegreePair;
import org.apache.flink.graph.utils.proxy.GraphAlgorithmWrappingDataSet;
import org.apache.flink.graph.utils.proxy.OptionalBoolean;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;

public class Triangles <K extends Comparable<K> & CopyableValue<K>, VV, EV>
extends GraphAlgorithmWrappingDataSet<K, VV, EV, Tuple3<K, K, EV>>{
	
	private OptionalBoolean sortTriangleVertices = new OptionalBoolean(false, false);
	
	public Triangles<K, VV, EV> setSortTriangleVertices(boolean sortTriangleVertices) {
		this.sortTriangleVertices.set(sortTriangleVertices);

		return this;
	}
	
	public DataSet<Tuple3<K, K, EV>> runInternal(Graph<K, VV, EV> input)
			throws Exception {
		
		// A gráf élein megy végig, feljegyzi a megfelelõ
		// csúcspárokat és a hozzájuk tartozó éleket.
		DataSet<Tuple3<K, K, EV>> filteredByID = input
				.getEdges()
				.flatMap(new FilterByID<K, EV>())
					.setParallelism(PARALLELISM_DEFAULT)
					.name("Filter by ID");
		
		// Felannotálja a gráf éleit a fokszámokkal, tartalmazza az él típusát is.
		DataSet<Edge<K, Tuple3<EV, LongValue, LongValue>>> pairDegree = input
				.run(new EdgeDegreePair<K, VV, EV>()
					.setParallelism(PARALLELISM_DEFAULT));
//		pairDegree.print();
		
		// Az elõbbi annotált datasetbõl szûr fokszám alapján,
		// mivel az elõzõben minden él kétszer szerepelt, oda-vissza.
		// Megtartja a többszörös éleket is.
		DataSet<Tuple3<K, K, EV>> filteredByDegree = pairDegree
				.flatMap(new FilterByDegree<K, EV>())
					.setParallelism(PARALLELISM_DEFAULT)
					.name("Filter by degree");
//		filteredByDegree.print();
		
		DataSet<Tuple5<K, K, K, EV, EV>> triplets = filteredByDegree
				.groupBy(0)
				.sortGroup(1, Order.ASCENDING)
				.reduceGroup(new GenerateTriplets<K, EV>())
					.name("Generate triplets");
//		triplets.print();
		DataSet<Tuple6<K, K, K, EV, EV, EV>> triangles = triplets
				.join(filteredByID, JoinOperatorBase.JoinHint.REPARTITION_HASH_SECOND)
				.where(1, 2)
				.equalTo(0, 1)
				.with(new ProjectTriangles<K, EV>())
					.name("Triangle listing");
		triangles.print();
//			if (sortTriangleVertices.get()) {
//				triangles = triangles
//					.map(new SortTriangleVertices<K>())
//						.name("Sort triangle vertices");
//			}

		
		return filteredByID;
	}
	/**
	 * Kiszûri azokat a csúcsokat, melyeknél a source id nagyobb mint a target id.
	 * @author Lehel
	 *
	 * @param <T>
	 * @param <ET>
	 */
	@ForwardedFields("0; 1")
	private static final class FilterByID<T extends Comparable<T>, ET>
	implements FlatMapFunction<Edge<T, ET>, Tuple3<T, T, ET>> {
		private Tuple3<T, T, ET> edge = new Tuple3<>();

		@Override
		public void flatMap(Edge<T, ET> value, Collector<Tuple3<T, T, ET>> out)
				throws Exception {
			if (value.f0.compareTo(value.f1) < 0) {
				edge.f0 = value.f0;
				edge.f1 = value.f1;
				edge.f2 = value.getValue();
				out.collect(edge);
			}
		}
	}
	
	@ForwardedFields("0; 1")
	private static final class FilterByDegree<T extends Comparable<T>, ET>
	implements FlatMapFunction<Edge<T, Tuple3<ET, LongValue, LongValue>>, Tuple3<T, T, ET>> {
		private Tuple3<T, T, ET> edge = new Tuple3<>();

		@Override
		public void flatMap(Edge<T, Tuple3<ET, LongValue, LongValue>> value, Collector<Tuple3<T, T, ET>> out)
				throws Exception {
			Tuple3<ET, LongValue, LongValue> degrees = value.f2;
			long sourceDegree = degrees.f1.getValue();
			long targetDegree = degrees.f2.getValue();

			if (sourceDegree < targetDegree ||
					(sourceDegree == targetDegree && value.f0.compareTo(value.f1) < 0)) {
				edge.f0 = value.f0;
				edge.f1 = value.f1;
				edge.f2 = value.getValue().f0;
				out.collect(edge);
			}
		}
	}
	
	@ForwardedFields("0")
	private static final class GenerateTriplets<T extends CopyableValue<T>, ET>
	implements GroupReduceFunction<Tuple3<T, T, ET>, Tuple5<T, T, T, ET, ET>> {
		private Tuple5<T, T, T, ET, ET> output = new Tuple5<>();

		private List<T> visited = new ArrayList<>();

		@SuppressWarnings("unchecked")
		@Override
		public void reduce(Iterable<Tuple3<T, T, ET>> values, Collector<Tuple5<T, T, T, ET, ET>> out)
				throws Exception {
			int visitedCount = 0;
			output.f3 = null;
			output.f4 = null;
			Iterator<Tuple3<T, T, ET>> iter = values.iterator();

			while (true) {
				Tuple3<T, T, ET> edge = iter.next();

				output.f0 = edge.f0;
				output.f2 = edge.f1;
				if (output.f3 == null) {
					output.f3 = (ET) ("(" + edge.f0 + "-" + edge.f1 + ": " + edge.f2 + ")");				
				} else {
					output.f4 = (ET) ("(" + edge.f0 + "-" + edge.f1 + ": " + edge.f2 + ")");
				}

				for (int i = 0; i < visitedCount; i++) {
					output.f1 = visited.get(i);
					out.collect(output);
				}

				if (!iter.hasNext()) {
					break;
				}

				if (visitedCount == visited.size()) {
					visited.add(edge.f1.copy());
				} else {
					edge.f1.copyTo(visited.get(visitedCount));
				}

				visitedCount += 1;
			}
		}
	}
	
	@ForwardedFieldsFirst("0; 1; 2")
	@ForwardedFieldsSecond("0; 1")
	private static final class ProjectTriangles<T, ET>
	implements JoinFunction<Tuple5<T, T, T, ET, ET>, Tuple3<T, T, ET>, Tuple6<T, T, T, ET, ET, ET>> {
		@Override
		public Tuple6<T, T, T, ET, ET, ET> join(Tuple5<T, T, T, ET, ET> triplet, Tuple3<T, T, ET> edge)
				throws Exception {
			return new Tuple6<>(triplet.f0, triplet.f1, triplet.f2, triplet.f3, triplet.f4, edge.f2);
		}
	}
	
	private static final class SortTriangleVertices<T extends Comparable<T>>
	implements MapFunction<Tuple3<T, T, T>, Tuple3<T, T, T>> {
		@Override
		public Tuple3<T, T, T> map(Tuple3<T, T, T> value)
				throws Exception {
			// by the triangle listing algorithm we know f1 < f2
			if (value.f0.compareTo(value.f1) > 0) {
				T temp_val = value.f0;
				value.f0 = value.f1;

				if (temp_val.compareTo(value.f2) <= 0) {
					value.f1 = temp_val;
				} else {
					value.f1 = value.f2;
					value.f2 = temp_val;
				}
			}

			return value;
		}
	}
	
	@Override
	protected String getAlgorithmName() {
		return Triangles.class.getName();
	}

	@Override
	protected boolean mergeConfiguration(GraphAlgorithmWrappingDataSet other) {
		return false;
	}

	
}
