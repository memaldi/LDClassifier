#!/bin/bash

ACTION=$1
INPUT=$2
OUTPUT=$3
LIMIT=$4

hadoop dfsadmin -safemode leave
hadoop fs -rmr $OUTPUT

hadoop jar LDClassifier.jar eu.deustotech.internet.ldclassifier.main.LDClassifier $ACTION $INPUT $OUTPUT $LIMIT
