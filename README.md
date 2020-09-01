# Detecting people with similar business taste from yelp review dataset  

## The project description
This project is a mini-project in class Data Mining. It is aimed at using the Girvan-Newman algorithm to detect communities in an efficient way within a distributed environment.

## Programming language and libraries
Python3.6, Scala 2.11 and Spark 2.3.2, only use Spark RDD and standard Python or Scala libraries.

## Procedure
- Constructed the social network graph to add the connection between users.
- Calculated each edge’s betweenness by taking every node individually as the root to build the BFS tree.
- Divided the graph into communities using the Girvan-Newman algorithm to reach the global highest modularity.
