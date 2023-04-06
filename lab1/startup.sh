source /etc/profile
hdfs namenode -format
/opt/hadoop-3.3.5/sbin/start-all.sh
hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/root
hdfs dfs -put input input
hdfs dfs -put input input2
hdfs dfs -rm -r output
yarn jar /home/vboxuser/git/mephi-bigdata/lab1/target/scala-2.13/lab1-assembly-0.1.0-SNAPSHOT.jar input input2 output
