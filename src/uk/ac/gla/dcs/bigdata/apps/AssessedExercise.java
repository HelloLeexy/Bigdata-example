package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

import uk.ac.gla.dcs.bigdata.studentstructures.ProcessedArticleWithInfo;
import uk.ac.gla.dcs.bigdata.studentfunctions.PreprocessFlatMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.SortMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.TermFrequencyCorpusAccumulator;
import uk.ac.gla.dcs.bigdata.studentfunctions.DPHmapGroup;
import uk.ac.gla.dcs.bigdata.studentfunctions.EliminateRedundancyMap;

/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {

	
	public static void main(String[] args) {
		
		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
		
		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[4]"; // default is local mode with two executors
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
	
		
		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries
		
		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		//if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v2.jl.fix.json";
		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);
		
		// Close the spark session
		spark.close();
		
		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {
			
			// We have set of output rankings, lets write to disk
			
			// Create a new folder 
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();
			
			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}
		
		
	}
	
	
	
public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		
		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article
		
		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle
		
		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		//----------------------------------------------------------------
		//cast<JavaRDD<Query>> broadcastQueries = new JavaSparkContext(spark.sparkContext()).broadcast(queriesRDD);
		
		// Use javaSparkContext to create a broadcast variable, in order to use the queries in the flatMap function
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		// Create the accumulator for "the sum of term frequencies for the term across all documents", so that we can improve the efficiency of the algorithm. 
		// Because the accumulator is a shared variable, which can be updated by all the executors.
		TermFrequencyCorpusAccumulator termFrequencyCorpusAccumulator = new TermFrequencyCorpusAccumulator();
		// Register the accumulator to the spark context, so that we can use it in the flatMap function.
		jsc.sc().register(termFrequencyCorpusAccumulator, "termFrequencyCorpusAccumulator");
		// Same as the termFrequencyCorpusAccumulator, we also need to create the accumulators for "the total number of documents in the corpus" and "the length of the document in the corpus".
		LongAccumulator totalDocsInCorpusAccumulator = jsc.sc().longAccumulator("totalDocsInCorpusAccumulator");
		LongAccumulator totalDocsLenCorpusAccumulator = jsc.sc().longAccumulator("totalDocsLenCorpusAccumulator");

		// Use collectAsList to get the list of queries, and then broadcast it to the executors. In this way, we can improve the efficiency by avoiding the repeated transmission of the queries.
		List<Query> queryList = queries.collectAsList();
		Broadcast<List<Query>> BCqueryList = jsc.broadcast(queryList);

		// Use the flatMap function to preprocess the articles, and encoder the result as a Dataset<ProcessedArticleWithInfo>, so that we can use the Dataset API to process the data.
		// During the flatMap function, we also update the accumulators for "the sum of term frequencies for the term across all documents", "the total number of documents in the corpus" and "the total length of the document in the corpus".
		Dataset<ProcessedArticleWithInfo> ProcessedArticleWithInfos = news.flatMap(new PreprocessFlatMap(BCqueryList, 
																	termFrequencyCorpusAccumulator, 
																	totalDocsInCorpusAccumulator, 
																	totalDocsLenCorpusAccumulator), 
																	Encoders.bean(ProcessedArticleWithInfo.class));

		// In this step, we use the previous Dataset API to trigger the execution of the flatMap function, so that we can get the result of the accumulators.
		// At the same time, we use the cache function to cache the result of the flatMap function, so that we can avoid the repeated execution of the flatMap function.
		ProcessedArticleWithInfos.cache();
		ProcessedArticleWithInfos.count();
		
		// Get the value of the accumulators, and then broadcast it to the executors, so that we can calculate the DPH score in the mapGroup function
		// BCtotalDocsInCorpus and BCtotalTermFrequencyInCorpus can be got directly from the accumulators, and BCaverageDocumentLengthInCorpus can be calculated by the values of them(division).
		Broadcast<Long> BCtotalDocsInCorpus = jsc.broadcast(totalDocsInCorpusAccumulator.value());
		Broadcast<Map<String, Integer>> BCtotalTermFrequencyInCorpus = jsc.broadcast(termFrequencyCorpusAccumulator.value());
		Broadcast<Double> BCaverageDocumentLengthInCorpus = jsc.broadcast((double) totalDocsLenCorpusAccumulator.value() / totalDocsInCorpusAccumulator.value());
		
		// System.out.println("totalDocsInCorpus: "+totalDocsInCorpusAccumulator.value());
		// System.out.println("totalTermFrequencyInCorpus: "+termFrequencyCorpusAccumulator.value());
		// System.out.println("taolDocumentLengthInCorpus: "+totalDocsLenCorpusAccumulator.value());
		
		// Use the groupsByKey function to group the articles by the query, and then use the mapGroups function to calculate the DPH score for each article.
		// This step it's the curtial step to improve the efficiency, because we can calculate the DPH score while we are grouping the articles by the query.
		Dataset<DocumentRanking> DocumentRankings = ProcessedArticleWithInfos
		.groupByKey((MapFunction<ProcessedArticleWithInfo,Query>) article -> article.getQuery(), Encoders.bean(Query.class))
		.mapGroups(new DPHmapGroup(BCtotalDocsInCorpus, BCtotalTermFrequencyInCorpus, BCaverageDocumentLengthInCorpus), Encoders.bean(DocumentRanking.class));
		// for (DocumentRanking dr : DocumentRankings.collectAsList()) {
		// 	System.out.println("attention!  "+dr.getResults().get(0).getDocid());
		// 	System.out.println("Score attention!  "+dr.getResults().get(0).getScore());
		// }	
		
		// Use the map function to sort the articles by the score, then we can come to the final function for eliminating the redundancy by similarity() and return the top 10 articles.
		Dataset<DocumentRanking> SortedDocumentRankings = DocumentRankings.map(new SortMap(), Encoders.bean(DocumentRanking.class));

		//get the final result by eliminating the redundancy and return the top 10 articles
		Dataset<DocumentRanking> finalDocRank = SortedDocumentRankings.map(new EliminateRedundancyMap(), Encoders.bean(DocumentRanking.class)); 
		//Collect the result as a list, so that we can return the result to the main function and save it as files in the folder "results".
		List<DocumentRanking> finalDocRankList = finalDocRank.collectAsList();
		
		// Close the JavaSparkContext for spark
		jsc.close();

		// Return the final result
		return finalDocRankList;
	}
	
	
}
