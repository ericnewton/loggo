#!/bin/sh
scp $HOME/workspace/loggo/assemble/target/loggo*.gz ec2-user@$1:tarballs
scp loggo2.sh ec2-user@$1:
ssh ec2-user@$1 sh -x ./loggo2.sh
