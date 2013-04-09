# PerformanceTest
===============

** Java program to test performance of doing N inserts and selects in :
* Mysql
* Mongodb
* Redis 
* Sqlserver
** And using queue systems : 
* ZeroMQ
* RabbitMQ 

## Usage

* To build executable :

    mvn clean compile assembly:single -DskipTests

* To run program : 

    cd target/
    java -Djava.library.path=/usr/local/lib -jar ptest-jar-with-dependencies.jar

* Example output :

    Inserted 50000 (1 producers / 10 consumers) in 0:00:08.425
    Retrieved 50000 (1 producers / 10 consumers) in 0:00:03.634

* Command line parameters
```
usage: ptest
 -D <property=value>     use value for given property (jvm arguments)
 -database <DATABASE>    set the DATABASE system to use, valid values :
                         [MYSQL, MONGODB, REDIS (default), SQLSERVER]
 -debug                  activate debug mode
 -help,--help            show this help
 -num,--num-events <N>   N: number of events (insert/select operations) to
                         do
 -queue <QUEUE>          set the QUEUE system to use, valid values :
                         [RABBITMQ, ZEROMQ (default)] or leave empty to don't use a
                         queue system
 -workers <N>            N: number of workers (threads/consumers) that
                         execute operations

```