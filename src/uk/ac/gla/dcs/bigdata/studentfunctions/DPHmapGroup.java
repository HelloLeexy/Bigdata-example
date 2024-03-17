package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.ProcessedArticleWithInfo;

/**
 * The DPHmapGroup is a MapGroupsFunction used to calculate DPH scores for a group of ProcessedArticleWithInfo objects corresponding to a specific Query.
 * To perform efficient calculations in parallel, we use mapGroups transformation.
 * 
 * @param BCtotalDocsInCorpus Broadcast variable for the total number of documents in the corpus.
 * @param BCtotalTermFrequencyInCorpus Broadcast variable for the total term frequency in the corpus.
 * @param BCaverageDocumentLengthInCorpus Broadcast variable for the average document length in the corpus.
 * @param query The query for which the DPH scores are calculated.
 * @return DocumentRanking object containing the query and a list of RankedResult objects.
 * 
 * @author Zhexi Ju
 */

public class DPHmapGroup implements MapGroupsFunction<Query, ProcessedArticleWithInfo, DocumentRanking>{

    // Broadcast variables.
    private Broadcast<Long> BCtotalDocsInCorpus;
	private Broadcast<Map<String, Integer>> BCtotalTermFrequencyInCorpus;
	private Broadcast<Double> BCaverageDocumentLengthInCorpus;

    // Constructor for DPHmapGroup.
    public DPHmapGroup(Broadcast<Long> BCtotalDocsInCorpus, Broadcast<Map<String, Integer>> BCtotalTermFrequencyInCorpus, Broadcast<Double> BCaverageDocumentLengthInCorpus) {
        this.BCtotalDocsInCorpus = BCtotalDocsInCorpus;
        this.BCtotalTermFrequencyInCorpus = BCtotalTermFrequencyInCorpus;
        this.BCaverageDocumentLengthInCorpus = BCaverageDocumentLengthInCorpus;
    }
    
    @Override
    public DocumentRanking call(Query query, Iterator<ProcessedArticleWithInfo> articles) throws Exception {
        List<RankedResult> results = new ArrayList<RankedResult>();
        
        // Retrieve the ProcessedArticleWithInfo from the while iterator.
        while(articles.hasNext()){
            ProcessedArticleWithInfo articleInfo = articles.next();
    
        // get broadcast value totalDocsInCorpus, totalTermFrequencyInCorpus, averageDocumentLengthInCorpus
            long totalDocsInCorpus = BCtotalDocsInCorpus.getValue();
            Map<String, Integer> totalTermFrequencyInCorpus = BCtotalTermFrequencyInCorpus.getValue();
            double averageDocumentLengthInCorpus = BCaverageDocumentLengthInCorpus.getValue();
        
        // Get currentDocumentLength and term-frequency map from the ProcessedArticleWithInfo.
            int currentDocumentLength = articleInfo.getDocLength();
            double DPHScore = 0;
            Map<String, Integer> termFreq = articleInfo.getTermFreq();

        // Calculate DPH score for each term in the document.
        // Get every termFrequencyInCurrentDocument from term-frequency map, now we have all of the 5 variables needed to calculate DPH score.
            for (String term : termFreq.keySet()) {
                int termFrequencyInCurrentDocument = termFreq.get(term);
                int totalTermFrequency = totalTermFrequencyInCorpus.get(term);
                DPHScore += DPHScorer.getDPHScore((short) termFrequencyInCurrentDocument, totalTermFrequency, currentDocumentLength, averageDocumentLengthInCorpus, totalDocsInCorpus);
            }

        // Get average DPH score by dividing it by the number of terms in the query.  
        // Create a RankedResult object with the calculated DPH score and add it to the results list.
            DPHScore = DPHScore / query.getQueryTerms().size();
            RankedResult result = new RankedResult(articleInfo.getArticle().getId(), articleInfo.getArticle(), DPHScore);
            results.add(result);
        }

        // Create a DocumentRanking object and return it.
        DocumentRanking ranking = new DocumentRanking(query, results);
        return ranking;
    }
}
