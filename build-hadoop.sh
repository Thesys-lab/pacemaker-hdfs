# build clean Hadoop binary package with native code support

# install prerequisite
sudo apt-get update \
  && sudo apt-get -y install openjdk-8-jdk \
  && sudo apt-get -y install maven \
  && sudo apt-get -y install build-essential autoconf automake libtool cmake \
                             zlib1g-dev pkg-config libssl-dev libsasl2-dev \
  && sudo apt-get -y install snappy libsnappy-dev \
  && sudo apt-get -y install bzip2 libbz2-dev \
  && sudo apt-get -y install libjansson-dev \
  && sudo apt-get -y install fuse libfuse-dev \
  && sudo apt-get -y install zstd

sudo apt-get -y install pdsh
sudo apt-get -y install libsnappy-dev
sudo apt-get -y install python3 python3-pip
pip3 install matplotlib numpy pandas


# install isa-l
sudo apt-get -y install nasm \
  && wget https://github.com/01org/isa-l/archive/v2.25.0.tar.gz \
  && tar zxf v2.25.0.tar.gz \
  && cd isa-l-2.25.0/ \
  && ./autogen.sh \
  && ./configure \
  && make -j4 \
  && sudo make install \
  && cd ..

# install protobuf 2.5.0
wget https://github.com/protocolbuffers/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.gz \
  && tar zxf protobuf-2.5.0.tar.gz \
  && cd protobuf-2.5.0 \
  && ./configure \
  && make -j4 \
  && make check \
  && sudo make install \
  && sudo ldconfig \
  && cd ..

# build Hadoop
export MAVEN_OPTS="-Xms256m -Xmx1536m"
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
mvn package -Pdist,native -DskipTests -Dtar -Dmaven.javadoc.skip=true \
    -Drequire.isal -Disal.lib=/usr/lib/ -Dbundle.isal \
    -Drequire.snappy -Dsnappy.lib=/usr/lib/x86_64-linux-gnu/ -Dbundle.snappy \
    -Drequire.openssl -Dopenssl.lib=/usr/lib/x86_64-linux-gnu/ -Dbundle.openssl

cp -R hadoop-dist/target/hadoop-3.2.0 /tmp/

# check native code support
cd /tmp/hadoop-3.2.0/ \
  && bin/hadoop checknative

