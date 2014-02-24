#!/bin/sh

# Copyright 2014 Informatica Corp.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [ "$JAVA_HOME" != "" ]; then
  JAVA="$JAVA_HOME/bin/java"
else
  JAVA=java
fi


BASEDIR=`dirname $0`
p=`pwd`
cd $BASEDIR/..
BASEDIR=`pwd`
cd "$p"
CLASSPATH="$BASEDIR/lib/*:$BASEDIR/conf"
LOGDIR=$BASEDIR/log
mkdir -p $LOGDIR
JVMARGS="-Dlogdir=$LOGDIR -Dlog4j.configuration=log4j.properties"


start_daemon(){
  node=$1
  shift
  pidfile=$LOGDIR/$node.pid
  outfile=$LOGDIR/$node.out
  if [ -f $pidfile ]
  then
    oldpid=`cat $pidfile`
    if kill -0 $oldpid 2>/dev/null
    then
      echo "Node $node already running with PID $oldpid. Please stop it with stop-node first"
      return
    else
      echo "Removing stale PID file $pidfile"
      rm $pidfile
    fi
  fi
  "$@" >> $outfile 2>&1  &
  pid=$!
  sleep 8
  if kill -0 $pid 2>/dev/null
  then
    echo "Started node $node"
    echo $pid > $LOGDIR/$node.pid
  else
    echo "Starting failed. Please see logfile for details"
  fi
}

stop_daemon(){
  node=$1
  shift
  pidfile=$LOGDIR/$node.pid
  if [ ! -f $pidfile ]
  then
    echo "PID file for node $node does not exist. Are you sure it is running?"
    return
  fi
  pid=`cat $pidfile`
  if kill -0 $pid 2>/dev/null
  then
    echo "Stopping node $node"
    kill -TERM $pid
    sleep 10
    if kill -0 $pid 2>/dev/null
    then
      kill -9 $pid
    fi
    echo "Stopped"
  else
    echo "Node $node is not running. Removing stale PID file"
  fi
  rm $pidfile
}


case $1 in
start)
	;;
start-node)
	shift
        node=$1
        if [ -z "$node" ]
	then
	  echo "Usage $0 start-node <nodename>" >&2
	  exit
	fi
        conf="$BASEDIR/conf/$node.conf"
        if [ ! -r "$conf" ]
        then 
            echo "Config file \"$conf\" for node \"$node\" not found or not readable" >&2
            exit
        fi
	start_daemon $node "$JAVA" \
	     "-Dnodename=$node" "-Dsuffix=-$node" -cp "$CLASSPATH" $JVMARGS com.informatica.surf.Surf "$conf"
	;;
stop-node)
	shift
        node=$1
        if [ -z "$node" ]
	then
	  echo "Usage $0 stop-node <nodename>" >&2
	  exit
	fi
	stop_daemon $node
	;;
dump-stream)
	shift
	node=$1
	if [ -z "$node" ]
	then
	  echo "Usage: $0 dump-stream nodename" >&2
	  exit
        fi
        conf="$BASEDIR/conf/$node.conf"
        if [ ! -r "$conf" ]
        then 
            echo "Config file \"$conf\" for node \"$node\" not found or not readable" >&2
            exit
        fi
	"$JAVA" -cp "$CLASSPATH" "-Dnodename=${node}-dumpstream" $JVMARGS com.informatica.surf.sample.DumpStream $conf
	;;
page-count)
	shift
	node=$1
	if [ -z "$node" ]
	then
	  echo "Usage: $0 dump-stream nodename" >&2
	  exit
        fi
        conf="$BASEDIR/conf/$node.conf"
        if [ ! -r "$conf" ]
        then 
            echo "Config file \"$conf\" for node \"$node\" not found or not readable" >&2
            exit
        fi
	"$JAVA" -cp "$CLASSPATH" "-Dnodename=${node}-pagecount" $JVMARGS com.informatica.surf.sample.PageCount $conf
	;;
*)
	echo "Usage: $0 {start-node|stop-node}" >&2
esac

