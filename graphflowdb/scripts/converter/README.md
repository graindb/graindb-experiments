# SNB to GF Converter

First, make sure you have maven installed. You can check using:
```sh
$ which mvn
```

For Ubuntu, you can install it by running:
```sh
$ sudo apt install maven
```

The second step is to download SNB data generator and Hadoop.

```sh
$ mkdir <new-ldbc-dir-path> && cd <new-ldbc-dir-path>
$ git clone https://github.com/ldbc/ldbc_snb_datagen.git
$ cd ldbc_snb_datagen && git checkout 4f296a61ddc9ff7525aa08557a92ef43209084c9 && cd ..
$ wget http://archive.apache.org/dist/hadoop/core/hadoop-2.6.0/hadoop-2.6.0.tar.gz
$ tar xf hadoop-2.6.0.tar.gz
```

Remove walkmod plugin from ldbc_snb_datagen pom.xml (https://github.com/ldbc/ldbc_snb_datagen/issues/43)
```
 <plugin> 
   <groupId>org.walkmod.maven.plugins</groupId> 
   <artifactId>walkmod-maven-plugin</artifactId> 
   <version>1.0.3</version> 
   <executions> 
     <execution> 
       <phase>generate-sources</phase> 
       <goals> 
         <goal>apply</goal> 
       </goals> 
       <configuration> 
         <chains>pmd</chains> 
         <properties>configurationFile=rulesets/java/basic.xml</properties> 
       </configuration> 
     </execution> 
   </executions> 
 </plugin> 
```

If you are using a Mac OSX, uncomment the following line in run.sh script (under ```<new-ldbc-dir-path>```).
```
# zip -d $LDBC_SNB_DATAGEN_HOME/target/ldbc_snb_datagen-0.2.7-jar-with-dependencies.jar META-INF/LICENSE
```

The generator script requires the following three environment variables to be set:
```sh
$ export GRAPHFLOW_HOME=<graphflow-project-root-path>
$ export LDBC_HOME=<new-ldbc-dir-path>
$ export JAVA_HOME=<java-path>
```

Next, go to the converter directory in the Graphflow project directory and run the script:
```sh
$ cd $GRAPHFLOW_HOME/scripts/converter
$ ./snb-convert.sh 0.1 <output-folder>
```

The first argument (0.1) is the scaling factor. 0.1 is the smallest possible graph that can be generated which has around 1.7M edges. Possible values of the scaling factor are 0.1, 0.3, 1, 3, 10 and so on. Personal computers with 8GB of RAM, will barely be able to generate graphs up to scale 1 (17M edges).

The second argument is the output folder. The output folder will contain two folders: one for the generated CSVs and one for the serialized graph. The serialized graph can easily be loaded into Graphflow using Java code:

```java
var directory = "<output-folder>/serialized/"
var indexStore = IOUtils.deserializeIndexStore(directory);
var graph = IOUtils.deserializeGraph(directory, indexStore);
var catalog = new CatalogFactory().make(directory, graph);
var query = QueryParser.parseQuery("MATCH (a:PERSON)-[e1:KNOWS]->(b:PERSON)",
    graph.getKeyStore());

var planner = new QueryPlanFactory(query, catalog, graph, indexStore);
var plans = planner.getAllPlans();
for(var plan : plans) {
    plan.init(graph, indexStore);
    plan.execute();
    logger.info(String.format("%s|%s", plan.getEstimatedICost(), plan.getOneLineOutput()));
}
```
