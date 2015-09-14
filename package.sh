export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar

name=$1

hadoop com.sun.tools.javac.Main ${name}.java -d build
jar -cvf ${name}.jar -C build/ ./

