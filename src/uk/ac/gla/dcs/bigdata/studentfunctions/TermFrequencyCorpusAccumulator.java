package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.util.AccumulatorV2;

/**
 * This class is used to accumulate the term frequency of the corpus.
 * The input is a String, which is the term to be added to the term frequency map.
 * The output is a Map<String, Integer>, which is the term frequency map of the corpus.
 * The term frequency map is a map that stores the frequency of each term in the corpus.
 * isZero() is used to check if the term frequency map is empty
 * reset() is used to reset the term frequency map in order to avoid the influence of the previous run
 * add() is used to add a term to the term frequency map
 * merge() is used to merge the term frequency map from different partitions
 * value() is used to get the term frequency map
 * 
 * @param term The input String, which is the term to be added to the term frequency map.
 * @param termFrequencyMap The output Map<String, Integer>, which is the term frequency map of the corpus.
 * @return The output Map<String, Integer>, which is the term frequency map of the corpus.
 * 
 * @author Zhexi Ju
 */

public class TermFrequencyCorpusAccumulator extends AccumulatorV2<String, Map<String, Integer>> {
    private Map<String, Integer> termFrequencyMap = new HashMap<String, Integer>();

    @Override
    public boolean isZero() {
        return termFrequencyMap.isEmpty();
    }

    @Override
    public AccumulatorV2<String, Map<String, Integer>> copy() {
        TermFrequencyCorpusAccumulator newAccumulator = new TermFrequencyCorpusAccumulator();
        newAccumulator.termFrequencyMap = new HashMap<String, Integer>(termFrequencyMap);
        return newAccumulator;
    }

    @Override
    public void reset() {
        termFrequencyMap.clear();
    }

    // The input is a String, which is the term to be added to the term frequency map.
    // The output is a Map<String, Integer>, which is the term-frequency map of the corpus.
    @Override
    public void add(String term) {
        if (termFrequencyMap.containsKey(term)) {
            termFrequencyMap.put(term, termFrequencyMap.get(term) + 1);
        } else {
            termFrequencyMap.put(term, 1);
        }
    }

    @Override
    public void merge(AccumulatorV2<String, Map<String, Integer>> other) {
        Map<String, Integer> otherMap = other.value();
        for (Map.Entry<String, Integer> entry : otherMap.entrySet()) {
            if (termFrequencyMap.containsKey(entry.getKey())) {
                termFrequencyMap.put(entry.getKey(), termFrequencyMap.get(entry.getKey()) + entry.getValue());
            } else {
                termFrequencyMap.put(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public Map<String, Integer> value() {
        return termFrequencyMap;
    }
}
