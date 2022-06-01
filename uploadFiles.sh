#first param: teamName
#second param: ssh-password
pscp -pw "$2" target/spark.candy-1.0.0.jar node-user@spark-sparktraining-ssh.azurehdinsight.net:"$1"/spark.candy-1.0.0.jar