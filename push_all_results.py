#!/usr/bin/python

# Usage:
# python pushresults.py asnID testpath acctlist => But I've hard-coded the values instead
# The acctlist is exported from Markus (download button on groups page for the assignment)
# and has this format:
#     A csv file with one line per group
#     Group name (or studentid if a solo) is the first token on each line.
# "SET" indicates every line that must be changed by hand to prepare for a 
# new term/assignment.

# A script to push test results to MarkUs
import hashlib
import base64
import urllib
import urllib2
import sys

# SET: The long string of numbers and letters that appears on the dashboard.
apikey="YTljNmE5ZDhmNDg4MjcyODg2M2Q2MzkwMzQwMzQxOTc="

# The short assignment id
# assignment = sys.argv[1]
# SET: replace this with the id from MarkUs.
assignment = 'A2'

# The path of the test results file to load
# sys.argv[2]
# SET: The location of all the results files to be uploaded
result_path = './final_results_25mar'

# The group name (not repo name)
# acct_list = sys.argv[3]
# SET: the file containing the list of groups which was exported from markus
# using the download button on the "groups" page for the assignment.
acct_list = '2'

# The location of MarkUs
# SET: replace baseurl with this term's URL.
baseurl = 'https://stanley.cdf.toronto.edu/markus/csc343-2012-01'
url = baseurl + '/api/test_results'


accts = open(acct_list)
for line in accts:
    L = line.strip().split(",")
    group_name = L[0]
    print group_name

    # SET: replace results_report.txt with the name of whatever file has the results to upload.
    # Or list of files.
    # For 20121, these filenames were all prepended with the group name.
    # Eg, we had c0sida.a2.results.log etc.
    L = [".a2.results.final", ".a2.results.log"]
    for s in L:
        filename = group_name + s
        result_file_name = result_path + '/' + group_name + '/' + filename
        print result_file_name
        sys.stdout.flush()
        f = open(result_file_name)
    
        # read the test results file
        file_content=f.read() 
    
        values = {}
        values['group_name'] = group_name
        values['assignment'] = assignment
        values['filename'] = filename
        values['file_content'] = file_content
    
        headers = {}
        headers['Authorization'] = "MarkUsAuth " + apikey
    
        data = urllib.urlencode(values)
        # print data
    
        # Construct the request
        req = urllib2.Request(url, data, headers)
        #print "req.data:\n", req.data, "\n- - - - - - - - -"
        #print req.get_data()
    
        # Send the request
        response = urllib2.urlopen(req)
    
        # Get the results
        the_page= response.read()
        print the_page
        sys.stdout.flush()
        
        response.close()
        f.close()
