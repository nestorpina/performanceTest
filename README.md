performanceTest
===============



usage: ptest
 -database <DATABASE>    set the DATABASE system to use, valid values :
                         [mysql, mongodb, redis, sqlserver]
 -debug                  activate debug mode
 -help,--help            show this help
 -num,--num-events <N>   N: number of events (insert/select operations) to
                         do
 -queue <QUEUE>          set the QUEUE system to use, valid values :
                         [rabbitmq, zeromq] or leave empty to don't use a
                         queue system
 -workers <N>            N: number of workers (threads/consumers) that
                         execute operations
