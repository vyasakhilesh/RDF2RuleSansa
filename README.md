# RDF2Rules using Spark framework

This work is implementation of RDF2Rule work 

```Wang, Z. and Li, J., 2015. RDF2Rules: Learning rules from RDF knowledge bases by mining frequent predicate cycles. arXiv preprint arXiv:1512.07734.```

We divided the whole algorithm into three major subtasks as follows: 

* Finding Frequent Predicate Cycles (FPCs)
* Adding Type Information to the entities in FPCs
* Rule Generation.

## System Architecture 
![alt text](https://github.com/Kunal-Jha/RDF2RuleSansa/blob/master/RDf2Rule.jpg)


## Usage
### With Spark Cluster
1. Build the application with Maven

  ```
  cd /path/to/application
  mvn clean package
  ```
2. Setup the Spark cluster
3. Pass the jar file to 
```
spark-submit 
```
### With ScalaIDE
1. Build the application with Maven

  ```
  cd /path/to/application
  mvn clean package
   ```
2. Right click on project and build with Maven clean in IDE
3. Go to the GraphOps Class and execute it. (You may need to comment or uncomment the code based on whether you want FPC, PC or TypeInfo)
