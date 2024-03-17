package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.spark.util.LongAccumulator;
import java.util.stream.Collectors;
import org.apache.spark.api.java.function.FlatMapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.ProcessedArticleWithInfo;
import org.apache.spark.broadcast.Broadcast;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

/**
 * This class is used to preprocess the newsarticles and the queries.
 * The preprocessed news article contains "the term frequency of the article" and "the length of the current document".
 * The input is a NewsArticle, which is the news article to be preprocessed.
 * The output is a ProcessedArticleWithInfo, which is the preprocessed news article with information.
 * 
 * Conditions:
 * 1.When calculating DPH for a document, only consider terms within the title and within ContentItem elements 
 * 	that have a non-null subtype and that subtype is listed as “paragraph”. 
 * 	Additionally, if an article has more than 5 paragraphs, only consider the first 5.
 * 2.The term frequency of the article is calculated by counting the number of times each term in the query appears in the article.
 * 
 * @param value The input NewsArticle, which is the news article to be preprocessed.
 * @param BCqueryList Broadcast variable for the list of queries.
 * @param termFrequencyCorpusAccumulator Accumulator for the term frequency of the corpus.
 * @param totalDocsInCorpusAccumulator Accumulator for the total number of documents in the corpus.
 * @param totalDocsLenCorpusAccumulator Accumulator for the total length of documents in the corpus.
 * @return The output ProcessedArticleWithInfo, which is the preprocessed news article with information.
 * 
 * @author Xianyao Li
 */

public class PreprocessFlatMap implements FlatMapFunction<NewsArticle,ProcessedArticleWithInfo>{

	private static final long serialVersionUID = -4310298469416574266L;
	private Broadcast<List<Query>> BCqueryList;
	private TermFrequencyCorpusAccumulator termFrequencyCorpusAccumulator;
	private LongAccumulator totalDocsInCorpusAccumulator;
	private LongAccumulator totalDocsLenCorpusAccumulator;
	
	public PreprocessFlatMap(Broadcast<List<Query>> BCqueryList, 
	TermFrequencyCorpusAccumulator termFrequencyCorpusAccumulator, 
	LongAccumulator totalDocsInCorpusAccumulator,
	LongAccumulator totalDocsLenCorpusAccumulator) {
		this.BCqueryList = BCqueryList;
		this.termFrequencyCorpusAccumulator = termFrequencyCorpusAccumulator;
		this.totalDocsInCorpusAccumulator = totalDocsInCorpusAccumulator;
		this.totalDocsLenCorpusAccumulator = totalDocsLenCorpusAccumulator;
	}
	

	@Override
	public Iterator<ProcessedArticleWithInfo> call(NewsArticle value) throws Exception {

		List<ProcessedArticleWithInfo> PAL = new ArrayList<ProcessedArticleWithInfo>();
		List<Query> queryList = BCqueryList.getValue();
		String title = value.getTitle();
		TextPreProcessor textPreProcessor = new TextPreProcessor();
		List<String> wordList = new ArrayList<String>();
		// Only consider terms within the title and within ContentItem elements.
		// Add the title to the wordList.
		wordList.addAll(textPreProcessor.process(title));
		// Add the first 5 paragraphs to the wordList
		// Use the stream function to filter the ContentItem elements that have a non-null subtype and that subtype is listed as “paragraph”.
		// Additionally, if an article has more than 5 paragraphs, only consider the first 5.
		// Use the map function to get the content of the ContentItem.
		// Collect the result into a string.
		wordList.addAll(textPreProcessor.process(value.getContents()
							.stream()
							.filter(content ->{
								if (content !=null 
								&& content.getContent() !=null 
								&& "paragraph".equals(content.getSubtype())) {
									return true;
								}
								else return false;})
							.limit(5)
							.map(ContentItem::getContent)
							.collect(Collectors.joining(" ")))); 
		// Calculate the length of the current document.
		int docLength = wordList.size();
		totalDocsLenCorpusAccumulator.add(docLength);
		// Calculate the term frequency of the article, if the article contains the term in the query then add the term to the term frequency map.
		// A boolean variable haveTerm is used to check if the article contains the term in the query.
		for (Query query : queryList) {
			boolean haveTerm = false;
			List<String> queryTerms = query.getQueryTerms();
			Map<String, Integer> infoMap = new HashMap<>();
			for (String word : wordList) {
				if (queryTerms.contains(word)) {
					haveTerm = true;
					termFrequencyCorpusAccumulator.add(word);
					if(infoMap.containsKey(word)) {
						infoMap.put(word, infoMap.get(word)+1);
					}
					else {
						infoMap.put(word, 1);
					}
				}
			}
			// If the article contains the term in the query and has the title, then add the article to the ProcessedArticleWithInfo list for further processing
			if(haveTerm && title != null){
				ProcessedArticleWithInfo ProcessedArticle = new ProcessedArticleWithInfo();
				ProcessedArticle.setArticle(value);
				ProcessedArticle.setDocLength(docLength);
				ProcessedArticle.setTermFreq(infoMap);
				ProcessedArticle.setQuery(query);
				PAL.add(ProcessedArticle);
			}
		}

		// Add the total number of documents in the corpus
		totalDocsInCorpusAccumulator.add(1);
		// Return the ProcessedArticleWithInfo list by iterator
		return PAL.iterator();
	}

}
