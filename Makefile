USER=$(shell whoami)

##
## Configure the Hadoop classpath for the GCP dataproc enviornment
##

HADOOP_CLASSPATH=$(shell hadoop classpath)

# WordCount1.jar: WordCount1.java
# 	javac -classpath $(HADOOP_CLASSPATH) -d ./ WordCount1.java
# 	jar cf WordCount1.jar WordCount1*.class	
# 	-rm -f WordCount1*.class

# prepare:
# 	-hdfs dfs -mkdir input
# 	curl https://en.wikipedia.org/wiki/Apache_Hadoop > /tmp/input.txt
# 	hdfs dfs -put /tmp/input.txt input/file01
# 	curl https://en.wikipedia.org/wiki/MapReduce > /tmp/input.txt
# 	hdfs dfs -put /tmp/input.txt input/file02

# filesystem:
# 	-hdfs dfs -mkdir /user
# 	-hdfs dfs -mkdir /user/$(USER)

# run: WordCount1.jar
# 	-rm -rf output
# 	hadoop jar WordCount1.jar WordCount1 input output

URLCount.jar: UrlCount.java
	javac -classpath $(HADOOP_CLASSPATH) -d ./ UrlCount.java
	jar cf UrlCount.jar UrlCount*.class
	-rm -f UrlCount*.class

prepare-urlcount:
	-hdfs dfs -mkdir input-urlcount
	curl https://en.wikipedia.org/wiki/Apache_Hadoop > /tmp/input.txt
	hdfs dfs -put /tmp/input.txt input-urlcount/file01
	curl https://en.wikipedia.org/wiki/MapReduce > /tmp/input.txt
	hdfs dfs -put /tmp/input.txt input-urlcount/file02

filesystem-urlcount:
	-hdfs dfs -mkdir /user
	-hdfs dfs -mkdir /user/$(USER)

run-urlcount: UrlCount.jar
	-rm -rf output-urlcount
	hadoop jar UrlCount.jar UrlCount input-urlcount output-urlcount




##
## You may need to change the path for this depending
## on your Hadoop / java setup
##
HADOOP_V=3.3.4
STREAM_JAR = /usr/local/hadoop-$(HADOOP_V)/share/hadoop/tools/lib/hadoop-streaming-$(HADOOP_V).jar

stream:
	-rm -rf stream-output
	hadoop jar $(STREAM_JAR) \
	-mapper Mapper.py \
	-reducer Reducer.py \
	-file Mapper.py -file Reducer.py \
	-input input -output stream-output





