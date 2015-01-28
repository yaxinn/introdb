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
tkCount_csv = open('token_counts.csv', 'w')
tkcnt_header = ['token', 'count']
writer_cnt = csv.DictWriter(tkCount_csv, fieldnames=tkcnt_header)
writer_cnt.writeheader()
n_count = []
while True:
	chunck = f.readlines(2000)
	if not chunck: break
	for l in chunck:
		l = l.strip().split(' ')
		writer_cnt.writerow({'token': l[1], 'count': l[0]})
		if l[1] in pop_name: n_count.append({'token': l[1], 'count': l[0]})
tkCount_csv.close()
f.close()


# create a new name_count.csv file
namecnt_csv = open('name_counts.csv', 'w')
namecnt_header = ['token', 'count']
writer_namecnt = csv.DictWriter(namecnt_csv, fieldnames=namecnt_header)
writer_namecnt.writeheader()

# write tokens_count.csv and name_count.csv
for n in n_count:
	writer_namecnt.writerow(n)
namecnt_csv.close()