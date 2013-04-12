#!/bin/bash
rm LDClassifier.jar
hadoop dfsadmin -safemode leave
hadoop fs -rmr linkeddata/output
hadoop fs -rmr linkeddata/objects
jar -cvf LDClassifier.jar -C bin/ .
zip -r LDClassifier.jar lib/*

hadoop jar LDClassifier.jar eu.deustotech.internet.ldclassifier.rdf2graph.RDF2Graph hedatuz
