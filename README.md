# PerformanceTest
===============

* Java program to test performance of doing N inserts and selects in :
  * Mysql
  * Mongodb
  * Redis 
  * Sqlserver
* And using queue systems : 
  * ZeroMQ
  * RabbitMQ
  * No queue - use database directly 

## Usage

* To build executable :
```
    mvn clean compile assembly:single -DskipTests

* To run program : 
```
   cd target/
   java -Djava.library.path=/usr/local/lib -jar ptest-jar-with-dependencies.jar

* Example output :
```
Using DATABASE: MYSQL, QUEUE: NONE, WORKERS: 10, EVENTS: 1000
Inserted 1000 (10 producers) in 0:00:00.191
Retrieved 1000 (10 producers) in 0:00:00.116

* Command line parameters
```
usage: java -Djava.library.path=/usr/local/lib -jar
            ptest-jar-with-dependencies.jar [OPTIONS]
 -database <DATABASE>    set the DATABASE system to use, valid values :
                         [MYSQL, MONGODB, REDIS (default), SQLSERVER]
 -debug                  activate debug mode
 -help,--help            show this help
 -n,--num-events <N>     N: number of events to execute (insert/select
                         operations). Default : 50000
 -queue <QUEUE>          set the QUEUE system to use, valid values :
                         [RABBITMQ, ZEROMQ (default)] or leave empty to
                         don't use a queue system
 -workers <N>            N: number of workers (threads/consumers) that
                         execute operations. Default : 10

## Dependencies

* You must have installed and started the database you want to use : REDIS, MONGODB, MYSQL or SQLSERVER
* You must have installed and started the queue system you want to use
* All connections we'll be to localhost, on the default ports
* MYSQL :
  * User to connect : root/<empty password>
  * You should have created the table 'testtable' on test schema :
```
 CREATE TABLE `test`.`testtable` (
  `id` VARCHAR(50)  NOT NULL,
  `json` TEXT  NOT NULL,
  PRIMARY KEY (`id`)
)
ENGINE = MyISAM;

* SQLSERVER :
  * The test must be executed on a windows machine, with the user logged having rights to access sqlserver
  * You should have created the table 'testtable' on test schema :
```
sqlcmd -E -Q "CREATE TABLE testtable ( id VARCHAR(50)  NOT NULL, json TEXT NOT NULL);"
sqlcmd -E -Q "ALTER TABLE testtable ADD PRIMARY KEY (id)"

  * Before building : 
    * Download driver from : http://www.microsoft.com/en-us/download/details.aspx?displaylang=en&id=11774
    * Add to local repository : mvn install:install-file -Dfile=sqljdbc4.jar -DgroupId=com.microsoft.sqlserver -DartifactId=sqljdbc4 -Dversion=4.0 -Dpackaging=jar
