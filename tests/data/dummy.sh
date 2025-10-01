#!/bin/sh

echo "reading input"
duration=$(< input.txt)
echo "sleeping for ${duration}"
sleep $duration

echo "writing output file"
echo $(($duration - 1)) > out.txt

echo "done"
