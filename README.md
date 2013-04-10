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
  * No queue - send operations directly to the database

## Usage

* To build executable :
```
    mvn clean package
```
* To run program : 
```
   chmod +x ptest.sh
   ./ptest.sh
```

* Example output :
```
Using DATABASE: MYSQL, QUEUE: NONE, WORKERS: 10, EVENTS: 1000
Inserted 1000 (10 producers) in 0:00:00.191
Retrieved 1000 (10 producers) in 0:00:00.116
```

* Command line parameters
```
usage: java -Djava.library.path=/usr/local/lib -jar
            ptest-jar-with-dependencies.jar [OPTIONS]
 -csv,--optput-csv      Output values for csv import/export
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
```

* Helper script to run all tests: ```./runAllTests.sh```
```
MYSQL,NONE,Java,Inserted 50000,7333,,10 producers
MYSQL,NONE,Java,Retrieved 50000,2981,,10 producers
MYSQL,NONE,Java,Inserted 100000,14469,,10 producers
MYSQL,NONE,Java,Retrieved 100000,5763,,10 producers
MYSQL,ZEROMQ,Java,Inserted 50000,9257,,1 producers / 10 consumers
MYSQL,ZEROMQ,Java,Retrieved 50000,3606,,1 producers / 10 consumers
MYSQL,ZEROMQ,Java,Inserted 100000,19391,,1 producers / 10 consumers
MYSQL,ZEROMQ,Java,Retrieved 100000,7039,,1 producers / 10 consumers
MYSQL,RABBITMQ,Java,Inserted 50000,22017,,1 producers / 10 consumers
...
```

## Dependencies

* You must have installed and started the database you want to use : REDIS, MONGODB, MYSQL or SQLSERVER
* You must have installed and started the queue system you want to use.
  * If using REDIS, we assume libraries are located on /usr/local/lib , if not, change the -Djava.library.path parameter accordingly
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
```

* SQLSERVER :
  * The test must be executed on a windows machine, with the user logged having rights to access sqlserver
  * You should have created the table 'testtable' on test schema :

```
sqlcmd -E -Q "CREATE TABLE testtable ( id VARCHAR(50)  NOT NULL, json TEXT NOT NULL);"
sqlcmd -E -Q "ALTER TABLE testtable ADD PRIMARY KEY (id)"
```

  * When using SQLSERVER, build prerequisites : 
     * Download driver from : http://www.microsoft.com/en-us/download/details.aspx?displaylang=en&id=11774
     * Add to local repository : mvn install:install-file -Dfile=sqljdbc4.jar -DgroupId=com.microsoft.sqlserver -DartifactId=sqljdbc4 -Dversion=4.0 -Dpackaging=jar
