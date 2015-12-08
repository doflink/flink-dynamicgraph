package org.apache.flink.graph.dynamicgraph;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class PointAssigner
implements JoinFunction<Tuple2<Tuple2<Long, Long>, Long>, Tuple2<Tuple2<Long, Long>, Long>, Tuple2<Tuple2<Tuple2<Long, Long>, Long>, Tuple2<Tuple2<Long, Long>, Long>>> {

/**
* 
*/
private static final long serialVersionUID = 1L;

@Override
public Tuple2<Tuple2<Tuple2<Long, Long>, Long>, Tuple2<Tuple2<Long, Long>, Long>> join(Tuple2<Tuple2<Long, Long>, Long> jdd1, Tuple2<Tuple2<Long, Long>, Long> jdd2) {

	//Tuple2<Tuple2<Long, Long>, Long> empty = new Tuple2<Tuple2<Long, Long>, Long> ();
return new Tuple2<Tuple2<Tuple2<Long, Long>, Long>, Tuple2<Tuple2<Long, Long>, Long>> (jdd1, (jdd2 != null)  ? jdd2 : null);
}
}