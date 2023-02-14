#!/bin/bash

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.

# must set HUDI_HOME to your hudi directory

build_hudi() {
	declare opt
  declare OPTARG
  declare OPTIND
	CLEAN_ARG=""
	SPARK_INPUT=$HUDI_SPARK_VERSION
	PINTEG_TEST=""
	PACKAGE_INPUT=""
	SCALA_VER=""
	while getopts 'cias:p:h' opt; do
	case "$opt" in
		c)
			CLEAN_ARG="clean"
			;;
		i)
			PINTEG_TEST="-Pintegration-tests"
			;;
		a)
			SCALA_VER="-Dscala-2.12"
			;;
		s)
			SPARK_INPUT="$OPTARG"
			;;

		p)
			PACKAGE_INPUT="$OPTARG"
			;;

		?|h)
			echo "Usage: $(basename $0) [-c] [-i] [-a] [-s SPARKVERSION] [-p SINGLEPACKAGE]"
  		echo "   -c  	            use mvn clean argument"
			echo "   -i                 use -Pintegration-tests"
			echo "   -a                 use -Dscala-2.12"
  		echo "   -s  SPARKVERSION   spark version to build with:"
			echo "       2.4"
			echo "       3.1"
			echo "       3.2"
			echo "       3.3"
			echo "   -p  SINGLEPACKAGE  build a single bundle:"
			echo "       cli   (hudi-cli-bundle)"
			echo "       spark (hudi-spark-bundle)"
			echo "       util  (hudi-utilities-bundle)"
			echo "       integ (hudi-hudi-integ-test-bundle)"
  		return
			;;
	esac
	done
	shift "$(($OPTIND -1))"

	if [ ! -d "$HUDI_HOME" ]; then
		echo "HUDI_HOME is not set to an existing directory. Please set to the location of your Hudi repo."
		return
	fi

	SPARK_ARG=""
	case $SPARK_INPUT in
		2.4)
			SPARK_ARG="-Dspark2.4 $SCALA_VER"
			;;
		3.1)
			SPARK_ARG="-Dspark3.1 -Dscala-2.12"
			;;
		3.2)
			SPARK_ARG="-Dspark3.2 -Dscala-2.12"
			;;
		3.3)
			SPARK_ARG="-Dspark3.3 -Dscala-2.12"
			;;
	esac

	PACKAGE_ARG=""
	case $PACKAGE_INPUT in
		cli)
			PACKAGE_ARG="-pl packaging/hudi-cli-bundle -am"
			;;
		spark)
			PACKAGE_ARG="-pl packaging/hudi-spark-bundle -am"
			;;
		util)
			PACKAGE_ARG="-pl packaging/hudi-utilities-bundle -am"
			;;
		integ)
			PACKAGE_ARG="-pl packaging/hudi-integ-test-bundle -am"
	esac

	HUDI_COMMAND="mvn $CLEAN_ARG package -T 2C -DskipTests $PINTEG_TEST $SPARK_ARG $PACKAGE_ARG"

	CHANGED_DIR=""
	if [ ! "$PWD" = "$HUDI_HOME" ]; then
		pushd $HUDI_HOME
		CHANGED_DIR="1"
	fi

	echo "mvn command: $HUDI_COMMAND"
	eval $HUDI_COMMAND

	if [ "$CHANGED_DIR" -eq "1" ]; then
	    popd
	fi
	export HUDI_SPARK_VERSION=$SPARK_INPUT
}