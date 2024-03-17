package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.Map;


import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

/**
 * Contains getters and setters for the article, docLength, query and termFreq.
 * 
 * @param article The input NewsArticle, which is the news article to be preprocessed.
 * @param docLength The length of the current document
 * @param query The input Query, which is the query for which the DPH scores are calculated.
 * @param termFreq The term-frequency map of the current document.
 * @return The output ProcessedArticleWithInfo, which is the preprocessed news article with information.
 * 
 * @author Xianyao Li
 */

public class ProcessedArticleWithInfo implements Serializable {
	private NewsArticle article;
    // The length of the current document
	private int docLength;
    private Query query;
    // The term-frequency map of the current document.
	private Map<String, Integer> TermFreq;
	
	public ProcessedArticleWithInfo() {}
	
    // Getter and Setter methods for article
    public NewsArticle getArticle() {
        return article;
    }

    public void setArticle(NewsArticle article) {
        this.article = article;
    }

    // Getter and Setter methods for docLength
    public int getDocLength() {
        return docLength;
    }

    public void setDocLength(int docLength) {
        this.docLength = docLength;
    }

    // Getter and Setter methods for termFreq
    public Map<String, Integer> getTermFreq() {
        return TermFreq;
    }

    public void setTermFreq(Map<String, Integer> termFreq) {
        this.TermFreq = termFreq;
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }
}

