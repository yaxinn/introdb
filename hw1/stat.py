import csv
import re
import sys

# read in popular names
fref = open(str(sys.argv[2]), 'r')
pop_name = {}
for n in fref.read().strip().split('\n'):
	pop_name[n.lower()] = 0
fref.close()

# read tokens
file_name = str(sys.argv[1])
f = open(file_name, 'r')
tkCount_csv = open('token_counts.csv', 'wb')
tkcnt_header = ['token', 'count']
writer_cnt = csv.DictWriter(tkCount_csv, fieldnames=tkcnt_header)
writer_cnt.writeheader()

# create a new name_count.csv file
namecnt_csv = open('name_counts.csv', 'wb')
namecnt_header = ['token', 'count']
writer_namecnt = csv.DictWriter(namecnt_csv, fieldnames=namecnt_header)
writer_namecnt.writeheader()

# write tokens_count.csv and name_count.csv
for l in f:
	l = l.strip().split(' ')
	writer_cnt.writerow({'token': l[1], 'count': l[0]})
	if l[1] in pop_name:
		writer_namecnt.writerow({'token': l[1], 'count': l[0]})

f.close()
tkCount_csv.close()
namecnt_csv.close()