# This Python file uses the following encoding: utf-8

import csv, re, sys, os
from string import punctuation

# file to read
f = open(str(sys.argv[1]), 'r')

# special strings
CONTENT_S = "*** START OF THE PROJECT"
CONTENT_E = "*** END OF THE PROJECT"
fields = ["Title: ", "Author: ", "Release Date: ", "EBook #", "Language: "]
fields_ebook = ["title", "author", "release_date", "ebook_id", "language", "body"]
fields_tokens = ["ebook_id", "token"]

### files to write
# ebooks.csv
ebook_csv = open('ebook.csv', 'w')
writer_eb = csv.DictWriter(ebook_csv, fieldnames=fields_ebook)
writer_eb.writeheader()

# tokens.csv
tokens_csv = open('tokens.csv', 'w')
writer_tk = csv.DictWriter(tokens_csv, fieldnames=fields_tokens)
writer_tk.writeheader()

n_fg, c_fg = True, False
while True:
	chunck = f.readlines(2000)
	for l in chunck:
		if n_fg:
			new = {"title": 'null', "author": 'null', "release_date": 'null', "ebook_id": 'null', "language": 'null', "body": []}		
			n_fg = False
		if fields[0] in l and not c_fg: new[fields_ebook[0]] = l.strip().split('Title:')[1].strip()
		elif fields[1] in l and not c_fg: new[fields_ebook[1]] = l.strip().split(': ')[1].strip()
		elif fields[2] in l and not c_fg:
			t = re.split(r'[:\[\]\#]*', l.strip())
			new[fields_ebook[2]] = t[1].strip()
			new[fields_ebook[3]] = t[3]
		elif fields[4] in l and not c_fg: new[fields_ebook[4]] = l.strip().split(': ')[1]
		elif CONTENT_S in l: # start of body
			c_fg = True
		elif CONTENT_E in l: 
			c_fg , n_fg = False, True
			new['body'] = ''.join(new['body'])
			writer_eb.writerow(new)
			body = re.sub(r'[^\x00-\x7f]',r' ',new['body'])
			body = re.compile(r'[\s0-9{}\n]+'.format(re.escape(punctuation))).split(body.strip())
			[writer_tk.writerow({'ebook_id': new['ebook_id'], 'token': i.lower()}) for i in body if i.isalpha()]
		elif c_fg: # inside the body
			new['body'].append(l)

# close all files
f.close()
ebook_csv.close()
tokens_csv.close()