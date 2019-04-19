# Big-Data-Apache-Spark

•	Technologies used: Kafka, Spark, Spark-Mlib
•	We were given an API, Guardian2, from which the news was to be streamed. 
•	Stream_producer.py program was also given, which was the PRODUCER part of the kafka streaming. (inputs = API key, fromDate, toDate)
•	What we had to do, was the offline training of the data, and real time classification of the new test data.
•	(Follow all the steps given in the ppt, on apache kafka and spark) Also download pycharm, this will make your life easier. 
•	Steps to follow - 
  1.	Start the zookeeper and kafka servers.
  Go to the directory where kafka is downloaded, and do the following
  
    [1] .bin/zookeeper-server-start.sh config/zookeeper.properties (in terminal 1)
  
    [2] .bin/kafka-server-start.sh config/server.properties (in terminal 2)

  2.	GET DATA FOR OFFLINE TRAINING. 
  Modify the streanm_producer.py program in such a way that the data streamed get saved into a text file. 
  Make another file – model.py, where you load the txt file and make it into a dataframe, and fit it to a pipeline model consisting of –     RegexTokenizer, StopWordRemover, TF-IDF, vectorizer, label indexer and a classifier. 
  Fit the training data to this model. And save the model – pickle or just save the model as .model file
  
  3.	REAL TIME CLASSIFICATION FOR TEST DATA.
  Write a consumer program, to consume the data produced by the stream_producer.py program (change the dates), run the model.py, and write the consumer program such a way that it classifies from the beginning of the streaming. 
  Pass the test data through the pipeline and fit it to the model. ML algorithms used - 
    a.	Logistic Regression
    
    b.	Naïve Bayes
    
    c.	Random Forest
