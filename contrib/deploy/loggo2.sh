error() {
  echo $*
  exit 1
}

rm -f tarballs/kafka*
( cd tarballs ; wget http://apache.mirrors.ionfish.org/kafka/0.8.2.2/kafka_2.10-0.8.2.2.tgz ) || error cannot get kafka
grep leader /etc/hosts | awk '{ print $2 }' >leaders || error cannot get leaders
grep worker /etc/hosts | awk '{ print $2 }' >workers || error cannot get workers
pscp.pssh -e /tmp/errs -h workers -H leader2 -H leader3 tarballs/kafka* tarballs/loggo* $HOME/tarballs || error cannot copy tar files
pssh -e /tmp/errs -h leaders -h workers '(cd install ; tar -xvf ../tarballs/loggo*)' || error cannot unpack loggo
pssh -e /tmp/errs -h leaders '(cd install ; tar -xvf ../tarballs/kafka*)' || error cannot unpack kafka
pssh -e /tmp/errs -h leaders ln -sf ~/install/loggo*/lib/loggo-bigjar*.jar ~/install/zoo*/lib || error cannot make links for zookeeper
pssh -e /tmp/errs -h workers -H leader3 ln -sf ~/install/loggo*/lib/*.jar ~/install/accumulo*/lib || error cannot make links for accumulo 
pssh -e /tmp/errs -h workers -h leaders ln -sf ~/install/loggo*/lib/loggo-bigjar*.jar ~/install/hadoop*/share/hadoop/common/lib || error cannot make links for hadoop
ssh -t leader3 sudo yum install -y pssh
( sleep 2 ; echo 'create /kafka ""' ) | ./install/zookeeper*/bin/zkCli.sh 
