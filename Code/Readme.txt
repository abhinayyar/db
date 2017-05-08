###############################
###########DB Project 2#######
##############################
Step 1  : Compile code using script : ./run.sh

Note : Step 1 requires StorageManager.so , if StorageManager.so is not architecture supported on your system
-> g++ -fPIC -shared -std=c++11 StorageManager.cpp -o StorageManager.so


Step 2: Once Step 1 is done , we have dbparser file created, which is executable for our tinySql
-> You can run code using ./dbparser <FILE NAME>

************
CODE INPUT
************
For Testing we have included two files
	1. SQLTESTFILE.txt : This is standard file contating all the queries for this project ./dbparser SQLTESTFILE.txt
	2. PROJECT1.txt : These are all the queries used in PROJECT1 ./dbparser PROJECT1.txt 

**************
CODE OUTPUT
*************

STANDARD OUTPUT -> 
	CONSOLE ( table and DiskIO, Execution Time)
Note : You can stop print for exection time stats by commenting MACRO SHOW_STATS in parse_tree.cc


1. If you want to write code to file , uncomment #PRINT_TO_FILE MACRO in parse_tree.cc file
2. If you want to see parse tree for the code, uncomment #PRINT_PARSE_TREE in dbparse.y file 


SAMPLE OUTPUT FLE
-> For output testing, we ran SQLTESTFILE.txt and prin the output in output.txt file
