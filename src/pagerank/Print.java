package pagerank;

import java.util.List;

import scala.Tuple2;

public class Print {
	public static <T>void comma(List<T> l) {
		if(!l.isEmpty()) {
			System.out.print(l.get(0));
			if(l.size() > 1)
				for(int i = 1; i < l.size(); i++)
					System.out.print(", " + l.get(i));
			System.out.println();
		}
	}

	public static <T>void comma(T[] arr) {
		if(arr.length > 0) {
			System.out.print(arr[0]);
			if(arr.length > 1)
				for(int i = 1; i < arr.length; i++)
					System.out.print(", " + arr[i]);
			System.out.println();
		}
	}

	public static void comma(int[] arr) {
		if(arr.length > 0) {
			System.out.print(arr[0]);
			if(arr.length > 1)
				for(int i = 1; i < arr.length; i++)
					System.out.print(", " + arr[i]);
			System.out.println();
		}
	}

	public static void comma(double[] arr) {
		if(arr.length > 0) {
			System.out.print(arr[0]);
			if(arr.length > 1)
				for(int i = 1; i < arr.length; i++)
					System.out.print(", " + arr[i]);
			System.out.println();
		}
	}

	public static void commaTup(List<Tuple2<List<Integer>, Double>> l1) {
		for(int k = 0; k < l1.size(); k++) {
			Tuple2<List<Integer>, Double> t2 = l1.get(k);
			if(t2._1.size() > 0) {
				System.out.print(t2._1.get(0));
				for(int i = 1; i < t2._1.size(); i++)
					System.out.print(", " + t2._1.get(i));
				System.out.println("   " + t2._2);
			}
		}
	}
	public static <T>void lns(List<T> l) {
		for(int i = 0; i < l.size(); i++)
			System.out.println(l.get(i));
	}

	public static <T>void commaOrdered(List<T> l, int[] order) {
		if(!l.isEmpty() && order.length == l.size()) {
			System.out.print(l.get(order[0]));
			if(l.size() > 1)
				for(int i = 1; i < l.size(); i++)
					System.out.print(", " + l.get(order[i]));
			System.out.println();
		}
	}
}
