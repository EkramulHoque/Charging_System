Billing engine/Real time charging is one of the components of business support solutions (B.S.S)
in telecom industry. Using big data ecosystem, our solution is capable of charging events coming
from 4G (fourth generation) of telecom and is adaptive to handle multiple devices creating
different event types in 5G (fifth generation). Our model considers only the mandatory fields
relevant for charging. Other complex scenarios like call forwarding are exempted

Environment variables:

SPARK_HOME=D:\Softwares\Anaconda3\envs\billingEngine\Lib\site-packages\pyspark
HADOOP_HOME=D:\Softwares\Anaconda3\envs\billingEngine\Lib\site-packages\pyspark

#winutils inside $HADOOP_HOME/bin for windows

#APP_HOME variable is used to identify location where config files are kept
APP_HOME=D:\BillingEngine\Mediation

Configure $APP_HOME by setting the project directory in the IDE or "Environment Varibale" from the system.

Set the PYTHON_PATH in your project directory

Configure the kafka server:

1. Open the server.properties from kafka/config directory

2. Change the following ip addresses in : listeners, zookeeper.connect

Launching kafka in windows:

1. Start zookeeper:
bin\windows\zookeeper-server-start.bat config/zookeeper.properties

2. Start kafka server:
bin\windows\kafka-server-start.bat config\server.properties

3. Start producer:(Simulator for project/producer console for local testing)
bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic cdr-10

4. Start consumer:
bin\windows\kafka-console-consumer.bat --bootstrap-server 207.23.221.244:9092 --topic cdr-10 --from-beginning

Starting Simulator:


1. Configure config.ini
	a) Set the server name : open cmd; type ipconfig; get the ip address and set it on the "SERVER_IP"
	b) Check the port or leave the default value of 9092
	c) Customer list and cell tower locations must be present in the $APP_HOME/doc folder

2. Start the zookeeper(for windows only).

3. Start the kafka server

4. Open "producer.py" from "Charging_system\org\sfu\billing\simulator\controller"

5. Run "producer.py"

6. For debugging use the consumer command to check the message feeded on the kafka.

Starting Listener:

1. Create Conda environment checkend in billingEngine.yaml file 

2. Add all relevant properties in config.ini file. These properties are self-explanatory


