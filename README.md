# Charging_system

Charging system is a project with telecom billing components residing in line with big data ecosystem. System listens to configured topics in kafka server and does rating of events at real time. Processed records are persisted and visualised.

#Configuration Files

config.yaml: Parameter for the CDR generation\
definitions.py: global variables used in the project

#Simulator

Directory: org\
Package: controller,module,utility
    
    Work Flow:
    1. data_loader.py : loads customer list and locations
    2. simulator.py: creates message with random location and datetime interval using
    data from data_loader.py
    3. producer.py: connects to Kafka server and sends the message created in simulator.py 