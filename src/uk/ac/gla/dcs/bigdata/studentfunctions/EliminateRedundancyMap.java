package uk.ac.gla.dcs.bigdata.studentfunctions;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import org.apache.spark.api.java.function.MapFunction;
import java.util.ArrayList;
import java.util.List;

/**
 * This class is used to eliminate the redundancy in the ranking results by the function similarity(). 
 * The input is a DocumentRanking object, which contains a Query and a list of rankedResult objects. 
 * The output is also a DocumentRanking object, which contains the same Query and a list of finalDocuments objects, which is the final ranking results that meet the conditions.
 * Conditions:
 * 1. The title of the article is not null.
 * 2. The title of the article is not too similar to the titles of other articles in the finalDocumentsList.
 * 3. Only the top 10 articles that meet the conditions will be returned.
 * 
 * @param value The input DocumentRanking object, which contains a Query and a list of rankedResult objects.
 * @return The output DocumentRanking object, which contains the same Query and a list of finalDocuments objects, which is the final ranking results that meet the conditions.
 * 
 * @author Ziyan Huang
 */

public class EliminateRedundancyMap implements MapFunction<DocumentRanking , DocumentRanking >{

    @Override
    public DocumentRanking call(DocumentRanking value) throws Exception {
        // Create a finalDocumentstList of ArrayLists, which will store the final ranking results and return the top 10 articles that meet the conditions.
        List<RankedResult> finalDocumentsList = new ArrayList<>(10);
        // Get the RankedResultList from the previous input
        List<RankedResult> rankedResultList = value.getResults();
        // Get the Query from the input
        Query query = value.getQuery();

        // Iterate through the RankedResultList from the input
        for (int i = 0; i < rankedResultList.size() && finalDocumentsList.size() < 10; i++) {
            // Get the RankedResult one by one
            RankedResult rankedResult = rankedResultList.get(i);
            // Get the title of the article from the RankedResult
            NewsArticle article = rankedResult.getArticle();
            String title = article.getTitle();

            // If the title is null, ignore this result
            if (title == null) {
                continue;
            }

            // Create a flag to determine whether to keep this result. True to keep, false to ignore.
            boolean flag = true;

            // Iterate through the finalDocumentsList to get the title of the newsarticle
            for (int j = 0; j < finalDocumentsList.size(); j++) {
                NewsArticle finalArticle = finalDocumentsList.get(j).getArticle();
                String finalTitle = finalArticle.getTitle();

                // Check if the title of the current ranking result is null. If not, calculate the similarity between the title of the current ranking result and the titles of other results in the list.
                if (finalTitle != null) {
                    double distance = TextDistanceCalculator.similarity(finalTitle, title);
                    // If the similarity is less than 0.5, set the flag to false, and the results that are too similar will be ignored.
                    if (distance < 0.5) {
                        flag = false;
                        break;
                    }
                } else {
                    // If the title of the current result is null, set the flag to false and ignore this result directly.
                    flag = false;
                    break;
                }
            }

            // Finally, check the value of the flag. If it is true, add the current ranking result to the finalDocumentsList, but only if the length of the list is less than 10.
            if (flag) {
                finalDocumentsList.add(rankedResult);
            }
        }

        // Create a new DocumentRanking object to store the final ranking results that meet the conditions, that is our final ordered articles.
        return new DocumentRanking(query, finalDocumentsList);
    }
}