package kmer;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Collections;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
//
import scala.Tuple2;
//
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
//
import kmer.SparkUtil;

public class Kmer {
  
   public static void main(String[] args) throws Exception {
      // STEP-1: handle input parameters
      if (args.length < 2) {
         System.err.println("Usage: Kmer <fastq-file> <K>");
         System.exit(1);
      }
	  
      final String fastqFileName =  args[0];
      final int K =  Integer.parseInt(args[1]); // to find K-mers

      // STEP-2: create a Spark context object
      JavaSparkContext ctx = SparkUtil.createJavaSparkContext("kmer");
      
      // broadcast K and N as global shared objects,
      // which can be accessed from all cluster nodes
      final Broadcast<Integer> broadcastK = ctx.broadcast(K);
      
      // STEP-3: read all transactions from HDFS and create the first RDD                   
      JavaRDD<String> records = ctx.textFile(fastqFileName, 1);
	  
		  
      // JavaRDD<T> filter(Function<T,Boolean> f)
      // Return a new RDD containing only the elements that satisfy a predicate.
      JavaRDD<String> filteredRDD = records.filter(new Function<String,Boolean>() {
        @Override
        public Boolean call(String record) {
			if(record.length() > 0){
				String firstChar = record.substring(0,1);
			if ( firstChar.equals("#") || firstChar.equals(">")) {
             return false; // do not return these records
			 }
			else {
				return true;
				}
				
			}
			else{ return false;}
			
			
        }
      });
	  
	  records = null;
      
      // STEP-4: generate K-mers
      // PairFlatMapFunction<T, K, V>     
      // T => Iterable<Tuple2<K, V>>
      JavaPairRDD<String,Integer> kmers = filteredRDD.flatMapToPair(new PairFlatMapFunction<
          String,        // T
          String,        // K
          Integer        // V
        >() {
         @Override
         public Iterator<Tuple2<String,Integer>> call(String sequence) {
            int K = broadcastK.value();         
            List<Tuple2<String,Integer>> list = new ArrayList<Tuple2<String,Integer>>();
            for (int i=0; i < sequence.length()-K+1 ; i++) {
                String kmer = sequence.substring(i, K+i);
                list.add(new Tuple2<String,Integer>(kmer, 1));
            }         
            return list.iterator();
         }
      });
	  
      filteredRDD = null;
    
      // STEP-5: combine/reduce frequent kmers
      JavaPairRDD<String, Integer> kmersGrouped = kmers.reduceByKey(new Function2<Integer, Integer, Integer>() {
         @Override
         public Integer call(Integer i1, Integer i2) {
            return i1 + i2;
         }
      });
	  
	  kmers = null;
      
	  //JavaPairRDD<String, Integer> sortedPairRDD = kmersGrouped.sortByKey();
	   
	  
	  kmersGrouped.saveAsTextFile("/kmers/output/" + K);
	  
      // done
      ctx.close();
      System.exit(0);
   }
}
