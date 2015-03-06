#!/bin/bash 
cd a2-allresults

#cd r1
total=`ls | wc -w`
echo 'Totoal Results: '$total
hasa2sql=`grep 'a2.sql exists.' * | wc -l`
echo 'Has a2.sql: '$hasa2sql
hasjava=`grep 'Assignment2.java exists.' * | wc -l`
echo 'Has Assignment2.java: '$hasjava
compiled=`grep 'JDBC connectDB Mark:' * | wc -l`
echo 'Can be compiled JAVA files: '$compiled
run=`grep 'JDBC Part Total Mark' * | wc -l`
echo 'Run without termination: '$run

function=(
'JDBC connectDB Mark:'
'JDBC insertCountry Mark:'
'JDBC getCountriesNextToOceanCount Mark:'
'JDBC getOceanInfo Mark:'
'JDBC deleteNeighbour mark:'
'JDBC listCountryLanguages Mark:'
'JDBC chgHDI Mark:'
'JDBC updateHeight Mark:'
'JDBC updateDB Mark:'
'JDBC disconnectDB Mark:'
)

IFS="|"

for((i=1;i<=10;i++))
do	
str="QUERY$i Mark:     5/5" 
avg=`grep ${str} * | wc -l`
percent=$(awk "BEGIN{print ${avg}/${hasa2sql}*100}")
echo "# of 5 in QUERY$i: "$avg/$hasa2sql ${percent}%
done

jdbcavg=`grep 'JDBC Part Total Mark' *  | awk '{sum+=$6} END {print "", sum/NR}'`
echo 'JDBC Part(successfully runned) Avg: '$jdbcavg

echo "Function name: Avg/# of 5/runnable:"
for((i=0;i<=9;i++))
do	
count=`grep ${function[$i]} *  | wc -l`
str="${function[$i]} 5 /5"
avg=`grep ${function[$i]} *  | awk '{sum+=$4} END {print "", sum/NR}'`
high=`grep ${str} *  | wc -l`
echo "${function[$i]} "${avg}/${high}/${count}
done