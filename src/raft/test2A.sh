#! /bin/zsh
clear
# VERBOSE=1 go test -run TestReElection2A | python dslogs.py -c 3
VERBOSE=1 go test -run 2A -race >> test.log
# python /home/bxiiiiii/6.824/src/raft/dslogs.py test.log -c 3
