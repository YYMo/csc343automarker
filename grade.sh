#!/bin/bash
# Main script to autotest assignment.
# Change parameters as necessary.


# Setup parameters
user='liudavid'
course='csc324-2014-09'
markus_url="https://markus.cdf.toronto.edu/${course}/"
# MarkUs repo list
markus_repo_list='res/markus_repos.txt'
# Destination of repo names list
repo_list='res/repo_list.txt'
# The following (a_num) is used in a two different places:
# (1) Assignment links
# (2) The order of the assignment in the CSV files
# Be sure to change the script if these two don't match
a_num='7' # MarkUs assignment id number
a_name='ex7' # MarkUs assignment short identifier
api_key='YWZmMTNhMTM5YTJiYjJhNzUwNWY0ZDM3YjgwNTk3Mzg=' # MarkUs API key (bottom of dashboard)

# Functions
checkin () {
    for group in repos/group*; do
        if [ -d "${group}/${a_name}" ]; then
            cd "${group}/${a_name}"
            #for r in result*; do
            for r in result*.txt; do
                svn add $r
                done
            svn ci -m "Autotest results" --username $user 
            cd ../../..
        fi
    done
}

upload () {
    for group in repos/group*; do
        cd "${group}/${a_name}"
        group_id=`expr substr ${group} 13 4`
        for r in result*; do
            content=`cat ${r}`
            echo "Authorization: MarkUsAuth ${api_key}"
            echo "${markus_url}api/assignments/${a_num}/groups/${group_id}/test_results.xml"
            curl -H "Authorization: MarkUsAuth ${api_key}" -F filename=$r -F "file_content=${content}" "${markus_url}api/assignments/${a_num}/groups/${group_id}/test_results.xml"
        done
        cd ../../..
    done
}

checkout () {
    # Update/clone repositories
    for repo in `cat ${repo_list}`; do
        G=`echo ${repo} | sed 's/.*\(group\_[0-9]*\)/\1/'`
        if [ -d "repos/$G" ]; then
            echo "${G}: updating repository"
            svn --username $user update "repos/${G}/${a_name}"
        else
            echo "${G}: cloning repository"
            svn --username $user checkout "${repo}/${a_name}" "repos/${G}/${a_name}"
        fi
    done
}

# Process options
download_repos=false
convert_repos=false
checkout_repos=false
pytest=false
hstest=false
rkttest=false
pltest=false
checkin_results=false
upload_results=false
record_marks=false

while getopts rcoa:ium opt
do
    case $opt in
        # download repo list from MarkUs?
        r)
            download_repos=true
            ;;
        # convert raw repo list?
        c)
            convert_repos=true
            ;;
        # checkout repositories from MarkUs?
        o)
            checkout_repos=true
            ;;
        # automark?
        a)
	    echo "Automarking was selected. Extra arg: $OPTARG"
	    case $OPTARG in
		python)
		    pytest=true
		    ;;
		racket)
		    rkttest=true
		    ;;
		haskell)
		    hstest=true
		    ;;
		prolog)
		    pltest=true
		    ;;
	    esac
            ;;
        # check in results to MarkUs repos?
        i)
            checkin_results=true
            ;;
        # Upload results using MarkUs API?
        u)
            upload_results=true
            ;;
        # Record marks?
        m)
            record_marks=true
            ;;
        # Invalid argument
        \?)
            echo "Invalid option: -$OPTARG"
            ;;
    esac
done


# Actual actions
if [ $download_repos = true ]; then
    echo "Downloading repo list from MarkUs..."
    python3 download_repo_list.py "${user}" "${pwd}" "${a_num}" > $markus_repo_list
fi

if [ $convert_repos = true ]; then
    echo "Converting repo list (from MarkUs format)..."
    python3 markusutil.py $markus_repo_list $repo_list
fi

if [ $checkout_repos = true ]; then
    checkout
fi

if [ $pytest = true ]; then
    automark_python $python_tests
fi

if [ $hstest = true ]; then
    automark_haskell $haskell_tests
fi

if [ $rkttest = true ]; then
    #automark_racket $racket_tests
    record_racket_marks $racket_tests
fi

if [ $pltest = true ]; then
    automark_prolog $prolog_tests
fi

if [ $checkin_results = true ]; then
    checkin
fi

if [ $upload_results = true ]; then
    upload
fi

if [ $record_marks = true ]; then
    record_python_mark $python_tests
fi
