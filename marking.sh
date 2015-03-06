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
students=`ls A2_late` #`ls A2` #$1  #a single student

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
allresultsPath=a2-allresults_late

##test the existence of the directory
if [ ! -d $allresultsPath ]
then
    mkdir $allresultsPath
fi


for name in $students
do
    echo $name
    path=$current/A2_late/$name/
    results=$current/A2_late/$name/result
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
        results=$name.$a2results
        rm $results
 
        touch $results
        
        delay=0

        echo $name >>$current/$list 

        echo "********Test the SQL part of Assignment 2 for $name********" >>$results
        echo >>$results

    
        echo *********Test existence of files**********>>$results
        echo >>$results
        if [ ! -f Assignment2.java ]; then
            echo "Unable to find Assignment2.java ... " >>$results
            else
            echo "Assignment2.java exists.">>$results
            jdbcn=`expr $jdbcn + 1`
        fi
        if [ ! -f a2.sql ]; then
            echo "Unable to find a2.sql ... ">>$results
            else
            echo "a2.sql exists.">>$results
            sqln=`expr $sqln + 1`
        fi
        echo >> $results


        #echo "********Create all the tables from a2.ddl************" >>$results
        psql -U $username -d $database -f $current/a2.ddl
        echo $?
        if [ "$?" != 0 ]
            then echo ********Failed to create tables from A2.DDL********>>$results 2>&1
        fi
        #echo "=============================================">>$results
        #echo >>$results

        #echo "********Populate the sample instances from A2-inserts.sql************" >>$results
        psql -U $username -d $database -f $current/sample.sql
        echo $?
        if [ "$?" != 0 ]
            then echo ********Failed to populate instances from A2-inserts.sql********>>$results 2>&1
        fi
        #echo "=============================================">>$results
        #echo >>$results


        echo "********Run a2.sql*********">>$results
        echo "********Run a2sql*********"
        sed '1 i\ SET search_path TO A2;' a2.sql > $current/temp.sql
        psql -U $username -d $database -f $current/temp.sql
        echo "=============================================">>$results
        echo >>$results
        rm -f $current/temp.sql
#       cp $results $current/$allresultsPath/$results.part1
#       exit

#Query1
        echo **********RUNNING THE SOLUTION**************
        psql -U $username -d $database -f $current/solution.sql

    
        echo "-----------------------------------">>$results
        echo "         TESTING FOR QUERY1        ">>$results
        echo "-----------------------------------">>$results
        echo "******** YOUR RESULT ********">>$results
            psql -U $username -d $database -c "select * from a2.Query1;" >> .table.your 2>&1
        #trap handle1 ERR
        less .table.your >> $results
        echo >>$results
        echo "******** EXPECTED RESULT ********">>$results
            psql -U $username -d $database -c "select * from a2.Query1Ans;" >> .table.expected 2>&1
        less .table.expected >> $results
        if [ -z `diff -q .table.your .table.expected` ]; then
            echo "QUERY1 Mark:     5/5">>$results
        else
            #echo `diff .table.your .table.expected` >> $results
            echo "QUERY1 Mark:     /5">>$results
        fi
        rm -f .table.your .table.expected 
            echo "=============================================">>$results
            echo >>$results

#Query2
        echo "-----------------------------------">>$results
        echo "         TESTING FOR QUERY2        ">>$results
        echo "-----------------------------------">>$results
        echo "******** YOUR RESULT ********">>$results
            psql -U $username -d $database -c "select * from a2.Query2;" >>.table.your 2>&1
        echo >>$results
        #trap handle2 ERR
        less .table.your >> $results
        echo >>$results
        echo "******** EXPECTED RESULT ********">>$results
            psql -U $username -d $database -c "select * from a2.Query2Ans;" >>.table.expected 2>&1
        less .table.expected >> $results
        if [ -z `diff -q .table.your .table.expected` ]; then
            echo "QUERY2 Mark:     5/5">>$results
        else
            #echo `diff .table.your .table.expected` >> $results
            echo "QUERY2 Mark:     /5">>$results
        fi
        rm -f .table.your .table.expected 
            echo "=============================================">>$results
            echo >>$results
        #trap - ERR
        #trap        
        
#Query3
        echo "-----------------------------------">>$results
        echo "         TESTING FOR QUERY3        ">>$results
        echo "-----------------------------------">>$results
        echo "******** YOUR RESULT ********">>$results
            psql -U $username -d $database -c "select * from a2.Query3;" >>.table.your 2>&1
        echo >>$results
        #trap handle3 ERR
        less .table.your >> $results
        echo >>$results
        echo "******** EXPECTED RESULT ********">>$results
            psql -U $username -d $database -c "select * from a2.Query3Ans;" >>.table.expected 2>&1
        less .table.expected >> $results
        if [ -z `diff -q .table.your .table.expected` ]; then
            echo "QUERY3 Mark:     5/5">>$results
        else
            #echo `diff .table.your .table.expected` >> $results
            echo "QUERY3 Mark:     /5">>$results
        fi
        rm -f .table.your .table.expected 
            echo "=============================================">>$results
            echo >>$results
        #trap - ERR
        #trap        
        
#Query4
        echo "-----------------------------------">>$results
        echo "         TESTING FOR QUERY4        ">>$results
        echo "-----------------------------------">>$results
        echo "******** YOUR RESULT ********">>$results
            psql -U $username -d $database -c "select * from a2.Query4;" >>.table.your 2>&1
        echo >>$results
        #trap handle4 ERR
        less .table.your >> $results
        echo >>$results
        echo "******** EXPECTED RESULT ********">>$results
            psql -U $username -d $database -c "select * from a2.Query4Ans;" >>.table.expected 2>&1
        less .table.expected >> $results
        if [ -z `diff -q .table.your .table.expected` ]; then
            echo "QUERY4 Mark:     5/5">>$results
        else
            #echo `diff .table.your .table.expected` >> $results
            echo "QUERY4 Mark:     /5">>$results
        fi
        rm -f .table.your .table.expected 
            echo "=============================================">>$results
            echo >>$results
        #trap - ERR
        #trap        
        
#Query5
        echo "-----------------------------------">>$results
        echo "         TESTING FOR QUERY5        ">>$results
        echo "-----------------------------------">>$results
        echo "******** YOUR RESULT ********">>$results
            psql -U $username -d $database -c "select * from a2.Query5;" >>.table.your 2>&1
        echo >>$results
        #trap handle5 ERR
        less .table.your >> $results
        echo >>$results
        echo "******** EXPECTED RESULT ********">>$results
            psql -U $username -d $database -c "select * from a2.Query5Ans;" >>.table.expected 2>&1
        less .table.expected >> $results
        if [ -z `diff -q .table.your .table.expected` ]; then
            echo "QUERY5 Mark:     5/5">>$results
        else
            #echo `diff .table.your .table.expected` >> $results
            echo "QUERY5 Mark:     /5">>$results
        fi
        rm -f .table.your .table.expected 
            echo "=============================================">>$results
            echo >>$results
        #trap - ERR
        #trap        
        
#Query6
        echo "-----------------------------------">>$results
        echo "         TESTING FOR QUERY6        ">>$results
        echo "-----------------------------------">>$results
        echo "******** YOUR RESULT ********">>$results
            psql -U $username -d $database -c "select * from a2.Query6;" >>.table.your 2>&1
        echo >>$results
        #trap handle6 ERR
        less .table.your >> $results
        echo >>$results
        echo "******** EXPECTED RESULT ********">>$results
            psql -U $username -d $database -c "select * from a2.Query6Ans;" >>.table.expected 2>&1
        less .table.expected >> $results
        if [ -z `diff -q .table.your .table.expected` ]; then
            echo "QUERY6 Mark:     5/5">>$results
        else
            #echo `diff .table.your .table.expected` >> $results
            echo "QUERY6 Mark:     /5">>$results
        fi
        rm -f .table.your .table.expected 
            echo "=============================================">>$results
            echo >>$results
        #trap - ERR
        #trap        
        
#Query7
        echo "-----------------------------------">>$results
        echo "         TESTING FOR QUERY7        ">>$results
        echo "-----------------------------------">>$results
        echo "******** YOUR RESULT ********">>$results
            psql -U $username -d $database -c "select * from a2.Query7;" >>.table.your 2>&1
        echo >>$results
        #trap handle7 ERR
        less .table.your >> $results
        echo >>$results
        echo "******** EXPECTED RESULT ********">>$results
            psql -U $username -d $database -c "select * from a2.Query7Ans;" >>.table.expected 2>&1
        less .table.expected >> $results
        if [ -z `diff -q .table.your .table.expected` ]; then
            echo "QUERY7 Mark:     5/5">>$results
        else
            #echo `diff .table.your .table.expected` >> $results
            echo "QUERY7 Mark:     /5">>$results
        fi
        rm -f .table.your .table.expected 
            echo "=============================================">>$results
            echo >>$results

        #trap - ERR
        #trap
        
#Query8
        echo "-----------------------------------">>$results
        echo "         TESTING FOR QUERY8        ">>$results
        echo "-----------------------------------">>$results
        echo "******** YOUR RESULT ********">>$results
            psql -U $username -d $database -c "select * from a2.Query8;" >>.table.your 2>&1
        echo >>$results
        #trap handle7 ERR
        less .table.your >> $results
        echo >>$results
        echo "******** EXPECTED RESULT ********">>$results
            psql -U $username -d $database -c "select * from a2.Query8Ans;" >>.table.expected 2>&1
        less .table.expected >> $results
        if [ -z `diff -q .table.your .table.expected` ]; then
            echo "QUERY8 Mark:     5/5">>$results
        else
            #echo `diff .table.your .table.expected` >> $results
            echo "QUERY8 Mark:     /5">>$results
        fi
        rm -f .table.your .table.expected 
            echo "=============================================">>$results
            echo >>$results

        #trap - ERR
        #trap        


#Query9
        echo "-----------------------------------">>$results
        echo "         TESTING FOR QUERY9        ">>$results
        echo "-----------------------------------">>$results
        echo "******** YOUR RESULT ********">>$results
            psql -U $username -d $database -c "select * from a2.Query9;" >>.table.your 2>&1
        echo >>$results
        #trap handle7 ERR
        less .table.your >> $results
        echo >>$results
        echo "******** EXPECTED RESULT ********">>$results
            psql -U $username -d $database -c "select * from a2.Query9Ans;" >>.table.expected 2>&1
        less .table.expected >> $results
        if [ -z `diff -q .table.your .table.expected` ]; then
            echo "QUERY9 Mark:     5/5">>$results
        else
            #echo `diff .table.your .table.expected` >> $results
            echo "QUERY9 Mark:     /5">>$results
        fi
        rm -f .table.your .table.expected 
            echo "=============================================">>$results
            echo >>$results

        #trap - ERR
        #trap

#Query10
        echo "-----------------------------------">>$results
        echo "         TESTING FOR QUERY10        ">>$results
        echo "-----------------------------------">>$results
        echo "******** YOUR RESULT ********">>$results
            psql -U $username -d $database -c "select * from a2.Query10 order by cname ASC;" >>.table.your 2>&1
        echo >>$results
        #trap handle7 ERR
        less .table.your >> $results
        echo >>$results
        echo "******** EXPECTED RESULT ********">>$results
            psql -U $username -d $database -c "select * from a2.Query10Ans order by cname ASC;" >>.table.expected 2>&1
        less .table.expected >> $results
        if [ -z `diff -q .table.your .table.expected` ]; then
            echo "QUERY10 Mark:     5/5">>$results
        else
            #echo `diff .table.your .table.expected` >> $results
            echo "QUERY10 Mark:     /5">>$results
        fi
        rm -f .table.your .table.expected 
            echo "=============================================">>$results
            echo >>$results

        #trap - ERR
        #trap

        echo ************Testing JDBC part ******************
        echo "************Testing JDBC part ******************" >>$results
        #echo "********Create all the tables from a2.ddl************" >>$results
        psql -U $username -d $database -f $current/a2.ddl
        echo $?
        if [ "$?" != 0 ]
            then echo ********Failed to create tables from A2.DDL********>>$results 2>&1
        fi
        echo "=============================================">>$results
        #echo >>$results

        #echo "********Populate the sample instances from sample.sql************" >>$results
        psql -U $username -d $database -f $current/sample.sql
        echo $?
        if [ "$?" != 0 ]
            then echo ********Failed to populate instances from A2-inserts.sql********>>$results 2>&1
        fi
        #echo "=============================================">>$results
        #echo >>$results

        #exit
        cp $current/TestAssignment2.java TestAssignment2.java
        javac TestAssignment2.java Assignment2.java >>$results 2>&1
        java -cp $current/postgresql-9.1-903.jdbc4.jar:. TestAssignment2 $database $username >>$results 2>&1
        rm -rf *.class 

        echo "">>$results
        echo "">>$results
        echo "<<<<<<<<<<<<<<<<<<<<<<<<<<<" >>$results
        echo "Part1 Total Mark :  /50">>$results
        echo "Part2 Total Mark :  /50">>$results
        echo "Total Mark       :  /100">>$results
        echo "<<<<<<<<<<<<<<<<<<<<<<<<<<<">>$results
        echo "">>$results
        echo "COMMENTS:">>$results

        cp $results $current/$allresultsPath/$results

    #else
        #echo *************** DIRECTORY CANNOT BE FOUND ************>>$results
    fi
done
echo $total
echo $jdbcn
echo $sqln
