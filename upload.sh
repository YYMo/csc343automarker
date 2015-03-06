#!/bin/bash
students=`ls all`
current=`pwd` 

for name in $students
do
    cd $current
    cp all/$name/$name.a2.results all_repo/$name/A2
    cd all_repo/$name
    svn add A2/$name.a2.results
    svn ci -m "add A2 result" --username t4yanyue

done

#for group in ; do
#    if [ -d "${group}/${a_name}" ]; then
#        cd "${group}/${a_name}"
#        #for r in result*; do
#        for r in result*.txt; do
#            svn add $r
#            done
#        svn ci -m "Autotest results" --username $user 
#    fi#done

