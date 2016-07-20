#!/bin/sh
str="input"
for (( i = 0; i < 59; i++ )); do
	cp input.txt ${str}${i}".txt"
done