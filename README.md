# Flink-DynamicGraph #
Large-scale Dynamic Graph Analysis

### What is this repository for? ###

* provide metrics to measure the similarity (differences) between dynamic graphs
* Now provide only dK-2 distance metric

###How to start everything?###

* Step1: Setup Flink, Hadoop Yarn
* Step2: Upload graphs (in edges format) to HDFS
* Step3: cd to /path/to/flink-dynamicgraph/, and compile:
  $mvn clean package
* Step4: Run:
  $run -c "org.apache.flink.graph.dynamicgraph.GraphSimilarityMetrics" target/flink-dynamicgraph-0.1.jar hdfs:///graph1.txt hdfs:///graph2.txt

### Contact? ###
* Do Le Quoc: do@se.inf.tu-dresden.de 
