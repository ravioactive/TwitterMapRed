============================================
   Twitter Data Analysis & User Clustering
============================================
DESCRIPTION:
Twitter is an ideal platform for studying vast amounts of spontaneously created data by millions of users all over the world. It is a hallmark of the popular voice because it’s content is rich in colloquial parlance making it a powerful tool to get an insight into the trending ideas among the community at large at any given time.
Data is pre-collected in text files through the Twitter Streaming API, the following analytical tasks are run on it in MapReduce:
* Word Count: Tokens of different types - hashtags, mentions, plain tokens, after general parsing/stop word removal.
* Word Co-occurence: The frequency of co-occurenct of 2 hashtags among all the tweets using the Pairs & Stripes algorithms,
* KMeans Clustering: On users based on their follower counts.
* Shortest Path: Modelling users as nodes, each consisting of node id, node distance and the adjacency list with the information of neighbor nodes, the shortest path between 2 nodes.

RUNNING THE JOBS
* To run the job, there is a pre-built jar file in the <FolderName>/out/artifacts/<FolderName>_jar directory.
You can run it directly as:
    hadoop jar /path/to/extracted/folder/code/out/artifacts/<FolderName>_jar
Copy the sample input files from <FolderName>/data into the hdfs directory by:
        hdfs dfs -copyFromLocal <FolderName>/data/<InputFile> /input/
* Sample Input for all the jobs is in the <FolderName>/data folder
* OUTPUT DETAILS:
    WORD COUNT:
    The outputs will be present as follows:
    * Word Count will be present in HDFS "/output/original" folder.
    * Sorted data in HDFS "/output/final_sort" folder.
    * The hash tags in HDFS "/output/final_hash" folder.
    * The sorted data of hash tags are found in HDFS "/output/final_hash/sort" folder.
    * The @tags are found in HDFS "/output/final_tweet" folder.
    * The sorted data of @tags are found in HDFS  "/output/final_tweet/sort" folder.

    WORD-COOCCURENCE:
    The outputs will be present as follows:
    * The Pairs algorithm in HDFS "/output/pair". The data is saved in two files as we are using two reducers.
    * The co-occurrence data by stripes algorithm is saved in HDFS
    "/output/stripes" folder.

    KMEANS:
    The code is provided in the "code/src" directory. There are 4 files:
        1. KMeans.java - the driver class.
            This job is controlled by 3 variables:
            INPUT_PATH - "/input" by default
            OUTPUT_PATH - "/output" by default
            NUM_CLUSTERS - Default 10
            Please change these variables as preferred, for e.g. NUM_CLUSTERS to change the number of clusters
        2. KMeansMapper - the Mapper
        3. KMeansReducer - the Reducer
        4. KMeansUtil - Helper functions
    The job does not take any command line arguments. All changes have to be made within the code and the jar has to be built again.
    Given the number of clusters, the final centroid values are given directly on the console. The job nonetheless outputs in the HDFS "/output/" folder

    SOCIAL GRAPH SHORTEST PATH:
    Out of the many folders with numbers as names, which contain data of each iteration. The folder with highest number contains the final output for the final iteration.

