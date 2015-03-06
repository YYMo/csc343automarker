#!/bin/bash
##################################################
# Before running this script, please connect to 
# a Postgres database server first:
#          ssh dbsrv1
# and change the 'database' and 'username' variables.
# you may need to change the jar file path (i.e. JDBC driver path)
# you may need to change the password field. The default is ""
# you need to create a schema called 'test'
##################################################


########Test a single student###########
#CHANGED by t1hussai
#CHANGED by t1wongka
students=`ls A2` #`ls A2` #$1  #a single student

a2results=a2.results 
current=`pwd` #current working path

########Change the following database name################
database=a2test

#######Change the following username####################
username=ta

total=0
sqln=0
jdbcn=0
##student list in current working path
list=studentList 

rm $list
touch $list

##the directory containing all test results
allresultsPath=a2-allresults

##test the existence of the directory
if [ ! -d $allresultsPath ]
then
    mkdir $allresultsPath
fi


for name in $students
do
    echo $name
    path=$current/all/$name/
    if [ -d $path ]
    then
        echo "find path"
    else
        path=$current/A2/$name/A2-late/
    fi
    
    if [ -d $path ] 
    then
        echo "************* DIRECTORY FOUND ***********"
        cd $path
        total=`expr $total + 1`
        rm TestAssignment2.java
 
        
    fi
done