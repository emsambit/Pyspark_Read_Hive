# Pyspark_Read_Hive
This script can be used in any project who wants to submit a pyspark job in yarn cluster mode

Many of us have encountered problem during spark job submit in cluster mode  that its not able to read hive table 

I have created the script which can be resused across any project , it can read from cluster hive-site.xml and  can pass to the pyspark code .

below is the directory structure 

Project----
        |
        |----------- conf
                             |-- contain the cfg file which will be having the spark setting and the hive-site.xml path
        |----------- pyfile
                             | -- contain all the python scripts (project and hive file reading)
        |----------- log
                             |-- create the log during  program execution
        | run.sh – the she script which will call all the other scripts  
