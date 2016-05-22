#!/usr/local/bin/fish

set BASEDIR (dirname (status -f))

if [ -z $SCALA_HOME ]
        echo "SCALA_HOME undefined"
        exit 3
end

cd $BASEDIR
rm -rf ./classes
mkdir -p ./classes
set countScala (ls -1 *.scala 2>/dev/null | wc -l)


if [ $countScala != 0 ]
        scalac -d ./classes *.java *.scala;
        and javac -d ./classes -classpath $SCALA_HOME/lib/scala-library.jar:./classes *.java;
        or begin
                echo "FAILURE TO BUILD"
                exit 1
        end
else
        javac -d ./classes *.java;
        or begin
                echo "FAILURE TO BUILD"
                exit 2
        end
end

echo "BUILD SUCCESSFUL"

# RUN TEST 1
if [ $countScala != 0 ]
        java -ea -cp $SCALA_HOME/lib/scala-library.jar:./classes OMVCCTest1
else
        java -ea -cp ./classes OMVCCTest1
end

# RUN TEST 2, with subtests 1 - 11
for TEST in (seq 11)
        if [ $countScala != 0 ]
                java -cp $SCALA_HOME/lib/scala-library.jar:./classes OMVCCTest2 $TEST
                # java -cp $SCALA_HOME/lib/scala-library.jar:./classes OMVCCTest2 $TEST > /dev/null 2>&1
        else
                java -cp ./classes OMVCCTest2 $TEST
                # java -cp ./classes OMVCCTest2 $TEST > /dev/null 2>&1
        end

        if [ $status != 0 ]
                echo "TEST $TEST: FAILED"
        else
                echo "TEST $TEST: PASSED"
        end
end

rm -rf ./classes

