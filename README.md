
+ Log level is taken care in src/main/resources/log4j.properties

+ As a feature, Spark does not store directly the output csv files to the desired name, e.g. aggrating.csv, but it will create a directory with the same name, and write csv (or other formats) in that directory.
An additional process may be needed if one wants a single file.

+ Execution on 700MB data
   - time 3.5 min
   - on a machine of 1.2 GB RAM available
   - 4 vcores
