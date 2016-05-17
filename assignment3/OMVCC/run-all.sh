#!/bin/sh

BASEDIR=$(dirname $0)

cd $BASEDIR
rm -rf ./classes
mkdir -p ./classes
countScala=`ls -1 *.scala 2>/dev/null | wc -l`
if [ $countScala != 0 ]; then 
	scalac -d ./classes *.java *.scala
	javac -d ./classes -classpath $SCALA_HOME/lib/scala-library.jar:./classes *.java
else
	javac -d ./classes *.java
fi 

if [ $countScala != 0 ]; then 
	eval "java -ea -cp $SCALA_HOME/lib/scala-library.jar:./classes OMVCCTest1"
else
	eval "java -ea -cp ./classes OMVCCTest1"
fi

testNum=1
for TEST in {1..4}
do
	if [ $countScala != 0 ]; then 
		eval "java -cp $SCALA_HOME/lib/scala-library.jar:./classes OMVCCTest2 $TEST"
		# eval "java -cp $SCALA_HOME/lib/scala-library.jar:./classes OMVCCTest2 $TEST > /dev/null 2>&1"
	else
		eval "java -cp ./classes OMVCCTest2 $TEST"
		# eval "java -cp ./classes OMVCCTest2 $TEST > /dev/null 2>&1"
	fi
	rc=$?
	if [[ $rc != 0 ]] ; then
	    echo "TEST $testNum: FAILED"
	else
		echo "TEST $testNum: PASSED"
	fi
	let testNum=testNum+1
done
rm -rf ./classes