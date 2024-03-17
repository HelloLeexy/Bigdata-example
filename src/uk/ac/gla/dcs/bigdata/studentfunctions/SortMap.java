package uk.ac.gla.dcs.bigdata.studentfunctions;


import java.util.Collections;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;

/**
 * This class is used to sort the ranking results in descending order based on the DPH score of the articles.
 * 
 * @param value The input DocumentRanking object, which contains a Query and a list of rankedResult objects.
 * @return The output DocumentRanking object, which contains the same Query and a list of rankedResult objects, but the list is sorted in descending order based on the DPH score of the articles.
 * 
 * @author Ziyan Huang
 */

public class SortMap implements MapFunction<DocumentRanking, DocumentRanking> {

    //Sort the ranking results in descending order based on the DPH score of the articles.
    @Override
    public DocumentRanking call(DocumentRanking doc) throws Exception {
        Collections.sort(doc.getResults(), (r1, r2) -> Double.compare(r2.getScore(), r1.getScore()));
        return doc;
    }
}
