#!/bin/bash

rm -f ./tmp
ls tmp
cat mr-out-* | LC_COLLATE=C sort | cat - >> tmp
cat tmp | wc -l
diff -s tmp ./mr-tmp/mr-out-0
