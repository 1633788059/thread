WORKDIR=/home/deploy

FEIYING_WOKESPACES=feiyingWorkspace

PROJECT_NAME=wangjubao.app.others

cd $WORKDIR

rm -rf $PROJECT_NAME/*

cd $WORKDIR/$FEIYING_WOKESPACES


cd $PROJECT_NAME

svn up

mvn clean install -Dmaven.test.skip -Denv=release

cd ./target

cp $PROJECT_NAME.tar.gz  $WORKDIR/$PROJECT_NAME

cd $WORKDIR/$PROJECT_NAME

tar -zxvf $PROJECT_NAME.tar.gz 

rm -rf $PROJECT_NAME.tar.gz

mvn com.alibaba.maven.plugins:maven-autoconf-plugin:0.5:autoconf  -Dproperties=/home/deploy/antx.properties

tar -zcvf $PROJECT_NAME.tar.gz *

scp $PROJECT_NAME.tar.gz deploy@10.239.16.59:/data/home/deploy/feiying.wangjubao.app

