#!/bin/bash
if [ `id -u` = 0 ]
then
    echo "********************************************************************"
    echo "** Error: root (the superuser) can't run this script!"
    echo "********************************************************************"
    exit 1
fi

cd `dirname $0`
BIN_DIR=`pwd`

cd ..
DEPLOY_HOME=`pwd`

HOST_NAME=`hostname`

if [ ! -r $BIN_DIR/env.sh ]; then
    echo "********************************************************************"
    echo "** Error: $BIN_DIR/env.sh not exist! "
    echo "** Please execute: "
    echo "**   cd $DEPLOY_HOME "
    echo "**   antxconfig ."
    echo "** or: "
    echo "**   mvn autoconf:autoconf"
    echo "********************************************************************"    
    exit 1
fi

#import home var env
. $BIN_DIR/env.sh

if [ ! -r $JAVA_HOME/bin/java ]; then
    echo "********************************************************************"
    echo "** Error: havana.javahome=$JAVA_HOME not exist!"
    echo "********************************************************************"    
    exit 1
fi

JAVA_OPTS=" -server -Xms256m -Xmx768m -XX:MaxPermSize=128m -XX:SurvivorRatio=2 -XX:+UseParallelGC "

export JAVA_OPTS=" $JAVA_OPTS -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true"

EXIST_PIDS=`ps  --no-heading -C java -f --width 1000 | grep "$DEPLOY_HOME" |awk '{print $2}'`
if [ ! -z "$EXIST_PIDS" ]; then
    echo "havana service server $HOST_NAME already started!"
    echo "PID: $EXIST_PIDS"
    exit;
fi

if [ ! -d $OUTPUT_HOME ]; then
	mkdir $OUTPUT_HOME
fi
if [ ! -d $LOG_ROOT ]; then
	mkdir $LOG_ROOT
fi

CONFIG_DIR=$DEPLOY_HOME/conf
LIB_JARS=$DEPLOY_HOME/lib/*
echo -e "Starting havana monitor server $HOST_NAME ...\c"
        LOG_DIR=$LOG_ROOT
        STORE_DIR=$STORE_PATH

		if [ ! -d $LOG_DIR ]; then
			mkdir $LOG_DIR
		fi
		if [ ! -d $STORE_PATH ]; then
			mkdir $STORE_PATH
		fi
	STDOUT_LOG=$LOG_DIR/buyerinfo-stdout.log
	nohup $JAVA_HOME/bin/java $JAVA_OPTS -classpath $CONFIG_DIR:$LIB_JARS com.wangjubao.app.others.buyerinfo.main.BuyerInfoMain >> $STDOUT_LOG 2>&1 &

echo "OK!"
START_PIDS=`ps  --no-heading -C java -f --width 1000 | grep "$DEPLOY_HOME" |awk '{print $2}'`
echo "DEPLOY_HOME: $DEPLOY_HOME"
cat /proc/$START_PIDS/environ | tr '\0' '\n'
echo "PID: $START_PIDS"
