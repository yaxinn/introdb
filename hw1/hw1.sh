#!/bin/bash
# bash command-line arguments are accessible as $0 (the bash script), $1, etc.
# echo "Running" $0 "on" $1
# echo "Replace the contents of this file with your solution."

python read.py $1

tail --line=+2 tokens.csv | cut -d ',' -f2 | grep -v "^\s*$" | sort | uniq -c | sed 's/^ *//' > temp.csv 
python stat.py temp.csv popular_names.txt
rm temp.csv

exit 0