# LinkageClustering
This package contains the code both to generate a agglomerative hierarchical clustering model and to check the goodness of that clustering using various internal validation indices in Spark and Scala. It is worth mentioning the implementation of several strategies to perform this hierarchical clustering ("min", "max" and "avg"), being able to choose each of them with a simple configuration parameter. This will be explained in more detail below.
## Getting Started
Within the "data" folder there are several databases with which to test the algorithm, all of them have been generated using the following [generator](https://github.com/josemarialuna/CreateRandomDataset) for clustering databases.
### Prerequisites
The package is ready to be used. You only have to download it and import it into your workspace. The main files include an example main that could be used. As for the classes with the most important code, we can find the following:
* Linkage: The class in charge of carrying out the LinkageModel of the database we want to study.
* Indexes: A class where are implemented all the internal validation indexes for clustering that can be used to measure the goodness of the clustering performed. If you want to extend the catalog of validation indexes, this would be the class to be modified for this purpose.
* MainLinkageExecution: The class prepared to carry out the desired hierarchical clustering of a database. With it, we save the LinkageModel generated from applying this clustering algorithm.
* MainClusterIndicesWithLinkageModelExecution: The class developed to check with all available internal validation indexes the goodness of the model for hierarchical clustering generated previously. Therefore, in order to use this class, it is very important that we already have the clustering model generated, otherwise it will not work correctly.
* MainClusterIndicesWithoutLinkageModelExecution: The class developed to check with all available internal validation indexes the goodness of the model for hierarchical clustering generated during its execution. It should be pointed out that in the case of using this class, it will not be necessary to have the hierarchical clustering model generated previously since this model is generated within this class before performing the calculations of the implemented internal validation indices.
## Execution
Within the 3 classes that exist at the user's disposal to test our hierarchical clustering algorithm, there are a series of common parameters for all of them and some that would only be necessary to introduce them in some of them. The following parameters are global for all executions:
* pathToDatabase: Route where is stored the database with which the algorithm is executed. It is necessary that all the columns have their attributes in "Double" format and that the database does not have a column either to designate the class to which each instance belongs or that could be used as an index. In other words, only databases with instances without class or identifier are valid.
* delimiter: Delimiter used in the database entered as a parameter to separate each of its columns.
* numCluster: Indicates the number of clusters that either has the past hierarchical clustering model as an argument or mark the limit as far as the hierarchical algorithm will go. Remember that the agglomerative hierarchical clustering has the quality of stopping until we have a desired number of clusters, so it would not be necessary to get to have all instances within a single cluster if the user does not want.
* numPoints: Number of instances that the database entered as parameter has.
* pathToCheckPoint: The path to set to perform checkpoints during the execution of parts of the algorithm. It is recommended to enter a path to HDD type storage. It is not necessary that the folder is created previously, otherwise the language itself will create the folder in the specified path.

If you only want to generate an agglomerative hierarchical clustering model, you would simply need to enter the following configuration parameters, in addition to those described in the previous paragraph: 
* linkageStrategy: Sets the desired strategy for carrying out hierarchical clustering. The user can choose between 3 implemented options: "min", "max" or "avg". Each one of them corresponds to "minimum or simple link grouping", "maximum or complete link grouping" and "average or average link grouping", respectively.
* pathToLinkageModel: Specifies the path where we will save the hierarchical agglomerative clustering model generated after the algorithm execution.

If we want to calculate the internal validation indices implemented and we have previously calculated the agglomerative hierarchical clustering model, we would only have to add the following configuration parameters to the global ones for their correct functioning:
* pathToLinkageModel: The route, where the hierarchical agglomerative clustering model generated previously, is stored.
* minimumCluster: Establishes the minimum number of clusters with which to calculate the implemented internal validation indices. It can never be lower than the one established both when generating the agglomerative hierarchical clustering model and when establishing the global parameter.
* maximumCluster: Establishes the maximum number of clusters with which to calculate the implemented internal validation indices.
* pathToResult: The path where will be stored the results of the calculations of all the internal validation indexes for all the clusters previously established.

If, on the other hand, we want to know the goodness of our agglomerative hierarchical clustering but we do not have a previously calculated model, it will be necessary to introduce the following configuration parameters, in addition to the global ones, for its correct operation:
* linkageStrategy: Sets the desired strategy for carrying out hierarchical clustering. The user can choose between 3 implemented options: "min", "max" or "avg". Each one of them corresponds to "minimum or simple link grouping", "maximum or complete link grouping" and "average or average link grouping", respectively.
* minimumCluster: Establishes the minimum number of clusters with which to calculate the implemented internal validation indices. It can never be lower than the one established both when generating the agglomerative hierarchical clustering model and when establishing the global parameter.
* maximumCluster: Establishes the maximum number of clusters with which to calculate the implemented internal validation indices.
* pathToResult: The path where will be stored the results of the calculations of all the internal validation indexes for all the clusters previously established.
## Result
If any of the above execution classes are used, the results will always be saved in the path specified during the configuration of any of them. However, the format of the saved data will be different depending on whether we are generating an agglomerative hierarchical clustering model or if we are calculating the goodness of that model generated by means of the implemented internal validation indices. The result of the model for an agglomerative hierarchical clustering will be saved with a file named "part-00000" inside the "Linkage" folder and will have the following format in each line:
* "cluster,point1,point2": Referring to each iteration of the algorithm that points or clusters have joined in the "cluster", that is, in each iteration, a new cluster is generated with the nearest points or groups of points in the distance.

While the results of the calculation of the goodness of the internal validation indices for our model will be found in the file "part-00000" of the folder "Resuts-Al_Indexes" and the format in each line will be:
* "K,silhouette,dunn,silhouettBD,dunnBD,davis-bouldin": Referring to the values of each of the indexes that are implemented for each of the "K" clusters to be tested by our algorithm. The order in which these indexes appear can be modified by the user if desired.
## Contributors
* José David Martín-Fernández.
* [José María Luna-Romera.](https://github.com/josemarialuna)
* José C. Riquelme Santos.
* Beatriz Pontes Balanza.
