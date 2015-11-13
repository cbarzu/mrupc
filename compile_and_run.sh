javac -cp `hadoop classpath` *.java
jar cvf Upc.jar *.class # crea el JAR
rm -r output
hadoop jar Upc.jar Upc input output
