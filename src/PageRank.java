import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import pagerank.Print;
import scala.Tuple2;

public class PageRank {
	static List<Double> r, r2;
	static final double beta = 0.8;
	static final int MAX_ITER = 20;
	public static void pagerank(SparkSession ss) throws Exception {
		JavaPairRDD<Tuple2<Integer, Iterable<Integer>>, Long> grouped = initGraph(ss, "graph_tiny.txt").distinct().groupByKey().sortByKey().zipWithIndex().cache();

		final int n = (int) grouped.count();

		final Hashtable<Integer, Integer> ht = getHashtable(grouped.mapToPair(x->new Tuple2<>(x._1._1, x._2)).collect());

		JavaPairRDD<Integer, Iterable<Integer>> indexed = grouped.mapToPair(x->new Tuple2<>(Math.toIntExact(x._2), x._1._2));

		class DegreeFunc implements PairFunction<Tuple2<Integer, Iterable<Integer>>, Integer, Tuple2<List<Integer>, Double>> {
			public Tuple2<Integer, Tuple2<List<Integer>, Double>> call(Tuple2<Integer, Iterable<Integer>> t) throws Exception {
				List<Integer> list_ints = new ArrayList<>();
				t._2.forEach(x->list_ints.add(ht.get(x)));
				Collections.sort(list_ints);
				return new Tuple2<>(t._1, new Tuple2<>(list_ints, 1.0/list_ints.size()));
			}
		}

		JavaPairRDD<Integer, Tuple2<List<Integer>, Double>> matrix = indexed.mapToPair(new DegreeFunc());

		r = new ArrayList<>(Collections.nCopies(n, 1.0/n));
		r2 = new ArrayList<>(Collections.nCopies(n, 1.0/n));

		for (int k=0; k<MAX_ITER; k++) {
				List<Double> next_r = new ArrayList<>(Collections.nCopies(n, 0.0));

				class MatFunc implements PairFlatMapFunction<Tuple2<Integer, Tuple2<List<Integer>, Double>>, Integer, Double> {
					public Iterator<Tuple2<Integer, Double>> call(Tuple2<Integer, Tuple2<List<Integer>, Double>> t) throws Exception {
						List<Integer> ints = t._2._1;

						List<Tuple2<Integer, Double>> list = new ArrayList<>();
						for(int i = 0; i < ints.size(); i++) {
							Double d = r.get(t._1) * t._2._2;
							list.add(new Tuple2<>(ints.get(i), d));
						}
						return list.iterator();
					}
				}
				JavaPairRDD<Integer, Double> r_not_summed = matrix.flatMapToPair(new MatFunc()).reduceByKey((v1, v2)->v1+v2).sortByKey();

				next_r = r_not_summed.map(x->x._2).collect();
				r = next_r;
				System.out.print(" r: " + k + ") ");
				Print.comma(r);


				List<Tuple2<Integer, Tuple2<List<Integer>, Double>>> mat = matrix.collect();
				List<Double> copy_r2 = new ArrayList<>(Collections.nCopies(n, 0.0));
				for(int z = 0; z < n; z++) {
					List<Integer> ints = mat.get(z)._2._1;

					for(int p = 0; p < ints.size(); p++) {
						Double d = mat.get(z)._2._2 * r2.get(z);
						int i = ints.get(p);
						copy_r2.set(i, copy_r2.get(i) + d);
					}
				}
				for(int i = 0; i < r2.size(); i++)
					r2.set(i, copy_r2.get(i));
				System.out.print("r2: " + k + ") ");
				Print.comma(r2);
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
		list.stream().forEach(x->ht.put(x._1, Math.toIntExact(x._2)));
		return ht;
	}

/* DEEP COPY R */
	static List<Double> copy(List<Double> orig) {
		List<Double> copy = new ArrayList<>(orig.size());
		for(int i = 0; i < orig.size(); i++)
			copy.add((double)orig.get(i));
		return copy;

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

/* Main / Standard Setup */
	public static void main(String[] args) throws Exception {
		SparkSession ss = settings();
		pagerank(ss);
		Thread.sleep(20000);
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