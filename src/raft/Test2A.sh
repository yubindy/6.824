#! /bin/zsh

bash ./test2A.sh > test.log
cat test.log |grep -E 'Passed|ok'