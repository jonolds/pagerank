import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class PagerankTEMP {
	
	static ArrayList<Double> r = new ArrayList();
	static final double beta = 0.8;
	static final int MAX_ITER = 40;

	public static void main(String[] args) throws Exception {
		JavaSparkContext sc = new JavaSparkContext("local[*]", "programname", 
	    		System.getenv("SPARK_HOME"), System.getenv("JARS"));

	    JavaRDD<String> graph = sc.textFile(args[0]);
	    JavaPairRDD<Integer, Integer> edges = graph.mapToPair(e -> getEdge(e));
	    JavaPairRDD<List<Integer>, Double> matrix = edges.distinct().groupByKey().flatMapToPair(p -> getDegree(p));

	    int n = (int) edges.groupByKey().count();
	    
	    // TODO Initialize vector r
	    
	    for (int i=0; i<MAX_ITER; i++) {
	    	
	    	// TODO Compute the new vector r and replace the old r with the new one.
	    	
	    }
	    
	    int[] sortedOrder = sort((ArrayList<Double>)r.clone());
	    
	    // Top 5 nodes with highest page rank
	    System.out.println(sortedOrder[n-1]+1+": "+r.get(sortedOrder[n-1]));
	    System.out.println(sortedOrder[n-2]+1+": "+r.get(sortedOrder[n-2]));
	    System.out.println(sortedOrder[n-3]+1+": "+r.get(sortedOrder[n-3]));
	    System.out.println(sortedOrder[n-4]+1+": "+r.get(sortedOrder[n-4]));
	    System.out.println(sortedOrder[n-5]+1+": "+r.get(sortedOrder[n-5]));
	    // Top 5 nodes with lowest page rank
	    System.out.println(sortedOrder[0]+1+": "+r.get(sortedOrder[0]));
	    System.out.println(sortedOrder[1]+1+": "+r.get(sortedOrder[1]));
	    System.out.println(sortedOrder[2]+1+": "+r.get(sortedOrder[2]));
	    System.out.println(sortedOrder[3]+1+": "+r.get(sortedOrder[3]));
	    System.out.println(sortedOrder[4]+1+": "+r.get(sortedOrder[4]));
	}

	// Selection sort. Return a list of indices in the ascending order.
	static int[] sort(ArrayList<Double> arr) {
		int[] order = new int[arr.size()];
		for (int i=0; i<arr.size(); i++) {
			order[i] = i;
		}
				
		for (int i = 0; i < arr.size() - 1; i++) {
			int index = i;
			for (int j = i + 1; j < arr.size(); j++)
				if (arr.get(j) < arr.get(index))
					index = j;

			double smallerNumber = arr.get(index);
			arr.set(index, arr.get(i));
			arr.set(i, smallerNumber);
			
			int smallerIndex = order[index];
			order[index] = order[i];
			order[i] = smallerIndex;
		}
		return order;
	}

	static Iterator<Tuple2<List<Integer>, Double>> getDegree(Tuple2<Integer, Iterable<Integer>> p) {
		// TODO Compute the values in matrix, which are given by 1/deg(i)
		
		return null;
	}

	static Tuple2<Integer, Integer> getEdge(String e) {
		String[] edge = e.split("\t");
		return new Tuple2<>(Integer.parseInt(edge[0]), Integer.parseInt(edge[1]));
	}	
}