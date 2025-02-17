	#!/bin/bash
count=$(grep -i "python"  inflating: question_tags.csv | wc -l)
echo "Number of lines containing 'python' in CSV files: $count"

