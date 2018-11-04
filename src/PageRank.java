import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

//@SuppressWarnings("unused")
public class PageRank {
	static double[] r;
	static final double beta = 0.8;
	static final int MAX_ITER = 40;

	public static void pagerank(SparkSession ss) throws Exception {
		JavaPairRDD<Integer, Iterable<Integer>> grouped = initGraph(ss, "graph_tiny.txt").distinct().groupByKey().sortByKey().cache();

		JavaPairRDD<Tuple2<Integer, Iterable<Integer>>, Long> indexed1 = grouped.zipWithIndex();

		final int n = (int) grouped.count();

		final Hashtable<Integer, Integer> ht = getHashtable(indexed1.mapToPair(x->new Tuple2<>(x._1._1, x._2)).collect());

		JavaPairRDD<Integer, Iterable<Integer>> indexed = indexed1.mapToPair(x->new Tuple2<>(Math.toIntExact(x._2), x._1._2));


		class DegreeFunc implements PairFunction<Tuple2<Integer, Iterable<Integer>>, List<Integer>, Double> {
			public Tuple2<List<Integer>, Double> call(Tuple2<Integer, Iterable<Integer>> t) throws Exception {
				List<Integer> list_ints = new ArrayList<>();
				t._2.forEach(x->list_ints.add(ht.get(x)));
				Collections.sort(list_ints);
				return new Tuple2<>(list_ints, 1.0/list_ints.size());
			}
		}

		JavaPairRDD<List<Integer>, Double> matrix = indexed.mapToPair(new DegreeFunc());


		grouped.saveAsTextFile("output/out1");
		matrix.saveAsTextFile("output/out2");


		r = new double[n];
		Arrays.fill(r, 1.0/n);

		List<Tuple2<List<Integer>, Double>> mat = matrix.collect();


		for (int k=0; k<2; k++) {
			double[] new_r = new double[n];

			for(int z = 0; z < n; z++) {
				List<Integer> ints = mat.get(z)._1;
				Double val = mat.get(z)._2;

				for(int p = 0; p < ints.size(); p++) {
					int i = ints.get(p);
					new_r[i] += r[z]*val;
				}

			}
			for(int i = 0; i < r.length; i++)
				r[i] = new_r[i];

		}

//		int[] sortedOrder = sort(copy(r));
	}

	// Selection sort. Return a list of indices in the ascending order.
	static int[] sort(ArrayList<Double> arr) {
		int[] order = new int[arr.size()];
		for (int i=0; i<arr.size(); i++)
			order[i] = i;

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

/* Print Output */
	static void printOut(int[] sortedOrder, int n, ArrayList<Double> r) {
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

	static Hashtable<Integer, Integer> getHashtable(List<Tuple2<Integer, Long>> list) {
		Hashtable<Integer, Integer> ht = new Hashtable<>(list.size());
		for(int i = 0; i < list.size(); i++)
			ht.put(list.get(i)._1, Math.toIntExact(list.get(i)._2));
		return ht;
	}


/* Initialize Graph */
	static JavaPairRDD<Integer, Integer> initGraph(SparkSession ss, String filename) {
		JavaRDD<String> graph = ss.read().textFile(filename).javaRDD();
		return graph.mapToPair(e -> getEdge(e));
	}

	static Tuple2<Integer, Integer> getEdge(String e) {
		String[] edge = e.split("\t");
		return new Tuple2<>(Integer.parseInt(edge[0]), Integer.parseInt(edge[1]));
	}

/* Copy centroid (DEEP COPY) */
	static <T>ArrayList<T> copy(ArrayList<T> cent) {
		return (ArrayList<T>)cent.stream().collect(Collectors.toList());
	}

/* Main / Standard Setup */
	public static void main(String[] args) throws Exception {
		SparkSession ss = settings();
		pagerank(ss);
//		Thread.sleep(20000);
		ss.close();
	}

	static SparkSession settings() throws IOException {
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);
		SparkSession.clearActiveSession();
		SparkSession spark = SparkSession.builder().appName("Kmeans").config("spark.master", "local").config("spark.eventlog.enabled","true").config("spark.executor.cores", "2").getOrCreate();
		SparkContext sc = spark.sparkContext();
		sc.setLogLevel("WARN");
		FileUtils.deleteDirectory(new File("output"));
		return spark;
	}
}



///* getDegree() */
//static Iterator<Tuple2<List<Integer>, Double>> getDegree(Tuple2<Integer, Iterable<Integer>> p) {
//	List<Tuple2<List<Integer>, Double>> list = new ArrayList<>();
//	List<Integer> list_ints = new ArrayList<>(Arrays.asList(p._1));
//	p._2.forEach(x->list_ints.add(x));
//	p._2.forEach(x->list.add(new Tuple2<>(list_ints, 1.0)));
//	return list.iterator();
//}