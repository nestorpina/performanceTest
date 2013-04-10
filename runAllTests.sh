#!/bin/bash

for DATABASE in MYSQL REDIS MONGODB;
do
  for QUEUE in NONE ZEROMQ RABBITMQ;
  do
	for EVENTS in 50000 100000; 
    do
      sh ptest.sh -database $DATABASE -queue $QUEUE -n $EVENTS -csv
    done
  done
done
