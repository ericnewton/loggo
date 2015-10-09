#!/bin/sh
usage() {
  echo usage: $0 hostname tarball 1>&2
  exit 1
}

if test -z "$1" || test -z "$2" 
then
   usage
fi
scp $2 ec2-user@$1:tarballs
scp loggo2.sh ec2-user@$1:
ssh ec2-user@$1 sh -x ./loggo2.sh
