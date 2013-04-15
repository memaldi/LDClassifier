#!/bin/bash
rm LDClassifier.jar
hadoop dfsadmin -safemode leave
#hadoop fs -rmr linkeddata/output
jar -cvf LDClassifier.jar -C bin/ .
zip -r LDClassifier.jar lib/*

hadoop jar LDClassifier.jar eu.deustotech.internet.ldclassifier.main.LDClassifier load linkeddata/input linkeddata/output
