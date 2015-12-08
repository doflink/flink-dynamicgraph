package org.apache.flink.graph.dynamicgraph;

import org.apache.flink.graph.dynamicgraph.PointAssigner;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.NullValue;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.example.utils.ExampleUtils;
import org.apache.flink.graph.Graph;




public class GraphSimilarityMetrics {
	
	private static boolean fileOutput = false;
	
	private static String edgesInputPath1 = null;
	
	private static String edgesInputPath2 = null;
	

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		/** Turn off log info messages **/
		env.getConfig().disableSysoutLogging(); 
		 
		/** create the graph **/
		Graph<Long, NullValue, NullValue> graph1 = Graph.fromDataSet(getEdgesDataSet(env, edgesInputPath1, fileOutput), env);		
		Graph<Long, NullValue, NullValue> graph2 = Graph.fromDataSet(getEdgesDataSet(env, edgesInputPath2, fileOutput), env);
		
		System.out.println("The dK2 distance (difference) between 2 graphs is: " + differenceGraphs(graph1, graph2));
	}
	
	/** parse parameters from user input **/
	private static boolean parseParameters(String[] args) {
		
		if(args.length > 0) {
			if(args.length != 2) {
				System.err.println("Usage: GraphDynamicMetrics <first input edges > <second input edges>");
				return false;
			}
	
			fileOutput = true;
			edgesInputPath1 = args[0];
			edgesInputPath2 = args[1];
		} else {
			System.out.println("Calculating joint degree distribution, and dk2-distance of 2 graphs ");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("Usage: GraphDynamicMetrics <first input edges> <second input edges> ");
		}
		return true;
	}
	
	public static DataSet<Tuple2<Tuple2<Long, Long>, Long>>  jointDegreeDistribution(Graph<Long, NullValue, NullValue> graph) throws Exception{
		
		/** get degree distribution of vertices **/
		DataSet<Tuple2<Long, Long>> verticesWithDegrees = graph.getDegrees();	
		
		DataSet<Tuple2<Long, Long>> edges = graph.getEdgeIds();
		
		DataSet<Tuple2<Tuple2<Long, Long>,Tuple2<Long, Long>>> egdesDegreeJoin = edges
				.join(verticesWithDegrees)
				.where("f0").equalTo("f0");
		
		DataSet<Tuple2<Long, Long>> firstMap = egdesDegreeJoin.map(new MapSecondField());	
		
		DataSet<Tuple2<Tuple2<Long, Long>,Tuple2<Long, Long>>> firstMapDegreeJoin = firstMap
				.join(verticesWithDegrees)
				.where("f0").equalTo("f0");	
		
		DataSet<Tuple2<Long, Long>> secondMap = firstMapDegreeJoin.map(new MapSecondField());	
		
		DataSet<Tuple2<Tuple2<Long, Long>, Long>> jdd = secondMap.map(new MapForCount()).groupBy(0).sum(1);
		
		jdd.print();		
		return  jdd;
		
	}
	


	
	public static double differenceGraphs(Graph<Long, NullValue, NullValue> graph1, Graph<Long, NullValue, NullValue> graph2) throws Exception{
		DataSet<Tuple2<Tuple2<Long, Long>, Long>> jdd1 = jointDegreeDistribution(graph1);
		DataSet<Tuple2<Tuple2<Long, Long>, Long>> jdd2 = jointDegreeDistribution(graph2);
		/** LeftOuterJoin is available from Flink version 0.10 **/
		DataSet<Tuple2<Tuple2<Tuple2<Long, Long>, Long>, Tuple2<Tuple2<Long, Long>, Long>>>
				jdd = jdd1.fullOuterJoin(jdd2).where("f0").equalTo("f0").with(new PointAssigner()); 	
				
		//DataSet<Tuple2<Tuple2<Tuple2<Long, Long>, Long>, Tuple2<Tuple2<Long, Long>, Long>>> jdd =
		//		jdd1.join(jdd2).where("f0").equalTo("f0");
		//jdd.print();
		DataSet<Long> difference = jdd.map(new EuclideanMap()).reduce(new IntSummer());		
		return  Math.sqrt(difference.collect().get(0));
	}
	
	/** ReduceFunction that sums Longs **/
	@SuppressWarnings("serial")
	public static class IntSummer implements ReduceFunction<Long> {		
		@Override
		public Long reduce(Long in1, Long in2) {
			Long result = in1 + in2;
			return result;
		}
	}
	
	/** Mapfunction is used to caculate Euclidean Distance between Join Degree Distribution of 2 Graphs **/
	@SuppressWarnings("serial")
	public static class  EuclideanMap implements MapFunction<Tuple2<Tuple2<Tuple2<Long, Long>, Long>, Tuple2<Tuple2<Long, Long>, Long>>, Long>{		
		private Long result = 0L; 
		
		@Override
		public Long map(Tuple2<Tuple2<Tuple2<Long, Long>, Long>, Tuple2<Tuple2<Long, Long>, Long>> in) {	
			if (in.f1 == null){
				if (in.f0.f0.f0 == in.f0.f0.f1){
					result = in.f0.f1 * in.f0.f1;
				}
				else {
					result = 2*in.f0.f1*in.f0.f1;
				}
				
			}
			else if(in.f0 == null){
				
				if (in.f1.f0.f0 == in.f1.f0.f1){
					result = in.f1.f1 * in.f1.f1;
				}
				else {
					result = 2*in.f1.f1*in.f1.f1;
				}
				
			}
			else{
				if (in.f0.f0.f0 == in.f0.f0.f1){
					result = (in.f0.f1 - in.f1.f1)*(in.f0.f1 - in.f1.f1);
				}
				else{
					result = 2*(in.f0.f1 - in.f1.f1)*(in.f0.f1 - in.f1.f1);
				}
			}
			
			return result ; 
		}
		
	}
		
	/** MapFunction that get second field values **/
	@SuppressWarnings("serial")
	public static class MapForCount implements MapFunction<Tuple2<Long, Long>,Tuple2<Tuple2<Long, Long>, Long>> {
		@Override
		public Tuple2<Tuple2<Long, Long>, Long> map(Tuple2<Long, Long> in) {	
			
			if (in.f0 > in.f1){
				Long tmp = in.f0;
				in.f0 = in.f1;
				in.f1 = tmp;
			}
			
			return new Tuple2<Tuple2<Long, Long>, Long>(in, 1L);
		}
	}
	
	/** MapFunction that get second field values **/
	@SuppressWarnings("serial")
	public static class MapSecondField implements MapFunction<Tuple2<Tuple2<Long, Long>,Tuple2<Long, Long>>,Tuple2<Long, Long>> {
		@Override
		public Tuple2<Long, Long> map(Tuple2<Tuple2<Long, Long>,Tuple2<Long, Long>> in) {			
			return new Tuple2<Long, Long>(in.f0.f1, in.f1.f1);
		}
	}
	
	
	// ******************************************************************************************************************
	// UTIL METHODS (Took from example.GraphMetrics of Gelly)
	// ******************************************************************************************************************
	
	static final int NUM_VERTICES = 100;
	
	static final long SEED = 9876;
	
	@SuppressWarnings("serial")
	private static DataSet<Edge<Long, NullValue>> getEdgesDataSet(ExecutionEnvironment env, String edgesInputPath, boolean fileOuput) {
		if (fileOutput) {
			return env.readCsvFile(edgesInputPath)
					.lineDelimiter("\n").fieldDelimiter("\t")
					.types(Long.class, Long.class).map(
							new MapFunction<Tuple2<Long, Long>, Edge<Long, NullValue>>() {
	
								public Edge<Long, NullValue> map(Tuple2<Long, Long> value) {
									return new Edge<Long, NullValue>(value.f0, value.f1, 
											NullValue.getInstance());
								}
					});
		} else {
			return ExampleUtils.getRandomEdges(env, NUM_VERTICES);
		}
	}
	
	
}


