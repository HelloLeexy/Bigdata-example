# Bigdata-AssCode

## Introduction
The task involves building an Apache Spark-based text search and filtering pipeline. 

The main objectives include:
1. preprocessing text documents and user-defined queries by removing stopwords and applying stemming. 
2. The documents are then ranked for each query using the DPH model. 
3. The pipeline also filters out overly similar documents, considering textual distance between titles, and ensures only the top 10 most relevant documents are retained for each query. 

The process aims to enhance search result relevance and eliminate redundancy in document rankings.

## Getting started
Required Java 11 / Maven

Download this GitLab code, and open src/uk.ac.gla.dcs.bigdata.apps/AssessedExercise.java (The mian clss which contains the main function)

Database Preparation:
We have provided a smaller database for code development and testing, as well as a larger database for validating the code.

- TREC_Washington_Post_collection.v3.example.json (38 MB)
- TREC_Washington_Post_collection.v2.jl.fix.json (5 GB)

Prior to usage, it is necessary to download the databases locally and configure the appropriate database entry points in the program.

## Main Component
(Provided) Spark Configuration and Session Creation:
Configure SparkConf by setting the master node and application name for Spark.
Create a Spark session using SparkSession, which will be used for executing Spark operations.

(Provided) Data Loading:
Load data from the given query and news article files.
Transform raw data into Query and NewsArticle objects.

(Part 1) Data Preprocessing "using FlatMap" Acceleration Strategy: Broadcasting Queries and Using Accumulators:
Utilize PreprocessFlatMap for preprocessing broadcasted queries and news, including stop word removal, stemming, and filtering irrelevant information.
To reduce the number of file traversals, we calculate values required for DPH (termFrequencyInCurrentDocument, totalTermFrequencyInCorpus, currentDocumentLength, averageDocumentLengthInCorpus, totalDocsInCorpus) using accumulators in this phase.
To minimize the use of For loops in subsequent processing, we concatenate NewsArticle, documentLength, and corresponding <Term, Frequency> outputs into ProcessedArticleWithInfo objects for further processing.

(Part 2) Computing DPH Scores "using groupByKey and mapGroups" Acceleration Strategy: Using Broadcasted totalDocsInCorpus, totalTermFrequencyInCorpus, and averageDocumentLengthInCorpus:
Apply the DPHmapGroup method to map Query to the respective values of totalDocsInCorpus, totalTermFrequencyInCorpus, and averageDocumentLengthInCorpus, and calculate the corresponding DPH scores.
Here, we directly push the results into the DocumentRanking object for further processing.

(Part 3) Sorting and Removing Redundancy Based on Title Similarity "using Map":
Use MapFunction to sort NewsArticle with DPH score.
Use EliminateRedundancyMap to filter out NewsArticle with empty titles and titles with similarity less than 0.5.

Results:
The result of top 10 relevant documents are returned for each query.
And write the final ranking results to the "results" folder with each query.

Environment Setup:
Set Hadoop directory and Spark master node through environment variables.
Retrieve the locations of query and news files from environment variables.

Program Highlights and Acceleration Strategies:
Utilization of broadcast variables (Broadcast) to efficiently transmit query data to various nodes.
Efficient document-level data aggregation using accumulators (Accumulator).
Cache the ProcessedArticleWithInfos dataset and then perform an operation (count()). This triggers the execution of preceding transformations and accumulators, ensuring their values are calculated.

## self-constructed class
# function
- class PreprocessFlatMap implements FlatMapFunction<NewsArticle,ProcessedArticleWithInfo>
- class DPHmapGroup implements MapGroupsFunction<Query, ProcessedArticleWithInfo, DocumentRanking>
- class SortMap implements MapFunction<DocumentRanking, DocumentRanking>
- class EliminateRedundancyMap implements MapFunction<DocumentRanking , DocumentRanking >
- class TermFrequencyCorpusAccumulator extends AccumulatorV2<String, Map<String, Integer>>

# structure
- class ProcessedArticleWithInfo implements Serializable
	private NewsArticle article;
	private int docLength;
  private Query query;
	private Map<String, Integer> TermFreq;

# contribution
- Xianyao Li
  PreprocessFlatMap.java
  ProcessedArticleWithInfo.java
- Zhexi Ju
  DPHmapGroup.java
  TermFrequencyCorpusAccumulator.java
- Ziyan Huang
  EliminateRedundancyMap.java
  SortMap.java

## Test and result
Test queries list:

[finance、

james bond、

on Facebook IPO]

Result list:

- finance

  1:cd2f4a30976a148b3311d93dfee9dd61 7.098280844515958 This new tool allows you to easily visualize 2016 campaign finance data

  2:9cf4b81c-7fe7-11e5-afce-2afd1d3eb896 7.03916067088458 Personal-finance courses in Virginia teach teens to budget in the real world

  3:653d18e2-81a3-11e1-bc36-069277cb6efc 7.014959873073102 John E. Petersen, economist and George Mason professor, dies at 71

  4:0ff91186-527f-11e6-88eb-7dda4e2f2aec 6.860423853514169 Congress agrees on changes that may make condos easier to buy and sell

  5:6f2e9fb4ffc6d0908ae3b0572715648a 6.824655534767293 Where do the Supreme Court’s campaign finance cases come from?

  6:2c28e04b645a58f9d3d3603e75b1f7c9 6.820256348058491 Should you waive the financing contingency?

  7:b3e2b15c-482d-11e4-b72e-d60a9229cc10 6.780752135053611 Montgomery Council approves plan for public finance of local campaigns

  8:5564972695db202eb818a296786196fb 6.733695116819052 The Washington Post introduces The Finance 202 newsletter

  9:5ef6cb62-1b7e-11e6-9c81-4be1c14fb8c8 6.712211105201856 FHA may soon play a larger role in financing of condos

  10:5d6497e8-a085-11e1-901a-9a6a92366aef 6.653051527972173 JPMorgan’s debacle, and its parallels to AIG


- james bond

  1:366851eade884f864f66e8736e0d9204 6.8951366095761735 No, a woman shouldn’t play James Bond

  2:1b663ee8f1ed00ed3426c9e3a84ad4db 6.859847075169448 What we talk about when we debate who should play James Bond

  3:102d7c1d810e147dc457e0012d3b4031 6.755276096950878 James Bond finally falls for a woman his own age

  4:2907546c-0e4a-11e2-a310-2363842b7057 6.571551120869212 James Bond’s character makes him endearing, if not realistic

  5:e021a6fda94976b7ee2516b77c818ea0 6.539397425017114 No, David Beckham will not be the next James Bond.

  6:9420f700-0e2a-11e2-bb5e-492c0d30bff6 6.357491437771167 James Bond’s drink is often imitated

  7:ebac22baff286ade4afb5581097a4719 6.267796762506883 A female James Bond?

  8:143c9430-0e5d-11e2-bb5e-492c0d30bff6 6.2539345149576935 James Bond turns 50

  9:76e706c8-2385-11e7-bb9d-8cd6118e1409 6.252358378087385 Clifton James, actor who portrayed sheriff in two Bond movies, dies at 96

  10:0e5bc194-1d0d-11e2-b647-bb1668e64058 6.183813035932675 Overtones of James Bond’s music


- on Facebook IPO

  1:a7c0dafc-ac26-11e1-84c1-ab1bdfe10472 8.16435223479237 Facebook stock performance, IPO said to be under investigation by SEC

  2:ceced4f4-4917-11e1-ba2c-44b5c309d24f 8.128981139278428 Report: Facebook to file IPO next Wednesday

  3:5ba537a6-a999-11e1-8e98-45f02ae6be92 8.059263025535532 Facebook hits new low, falls below $30

  4:a96785a0-4d98-11e1-a449-94121444c392 7.954354850165927 Facebook: 1,000 new millionaires?

  5:74b8c5ee-8a1f-11e1-90a9-fe032e7b7b2d 7.9143331183897345 Facebook said to pick date for IPO

  6:d66f7702af9ee3338ffca213e30e169d 7.841855103891162 No, getting a high price for its shares wouldn’t be a disaster for Twitter

  7:ec4a1c88-a104-11e1-9f5c-26b5233e3b65 7.780558696809091 Facebook IPO hits Nasdaq hiccup

  8:953e079a8d58c6782a98a7168828573d 7.736695370240385 Twitter’s IPO: The tweet heard ’round the world

  9:73f5c5de-9ebb-11e1-bca6-727bcbdbf866 7.736010195573593 As Facebook’s IPO nears, half of Americans say shares are overpriced

  10:21908e20-4c48-11e1-8fca-c9a169528fe4 7.691027809765965 Facebook IPO: How big will it be and what’s the significance?