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
# still a work in progress

run_hudi() {
	declare opt
  declare OPTARG
  declare OPTIND
	K_ARG=""
	HUDI_VERSION_INPUT=""
	JAR_ARG="0"
	PACKAGE_ARG="0"
	EXTRA_SPARK_CONFS=""
	SHELL_TYPE=""
	while getopts 'pjmv:k:c:q:' opt; do
	case "$opt" in
    v)
			HUDI_VERSION_INPUT="$OPTARG"
			;;
		j)
			JAR_ARG="1"
			;;
		p)
			PACKAGE_ARG="1"
			;;
		k)
			K_ARG="$OPTARG"
			;;
		c)
			EXTRA_SPARK_CONFS+=" $OPTARG"
			;;
		q)
			SHELL_TYPE="$OPTARG"
			;;
		m)
			EXTRA_SPARK_CONFS+=" --conf 'spark.sql.catalogImplementation=in-memory'"
			;;
		?|h)
      echo "still a WIP"
			;;
	esac
	done
	shift "$(($OPTIND -1))"


	echo "$K_ARG , $HUDI_VERSION_INPUT , $JAR_ARG , $PACKAGE_ARG , $EXTRA_SPARK_CONFS , $SHELL_TYPE"
	if [ -z "$HUDI_SPARK_VERSION" ]; then
		echo "HUDI_SPARK_VERSION not set. use \"set_spark\""
		return
	fi

	RUNNABLE_LOC=""
	if [ "$JAR_ARG" -eq "$PACKAGE_ARG" ]; then
		if [ "$JAR_ARG" -eq "1" ]; then
			echo "Both -j and -p flags set. Please only set one."
			return
		fi
		echo "Neither -j or -p flags set. Please use one."
		return
	elif [ "$JAR_ARG" -eq "1" ]; then
		if [ -z "$K_ARG" ]; then
			K_ARG=$(hudi_spark_bundle)
		fi
		RUNNABLE_LOC="--jars $K_ARG"
	elif [ "$PACKAGE_ARG" -eq "1" ]; then
		if [ -z "$K_ARG" ]; then
			if [ -z "$HUDI_VERSION_INPUT" ]; then
				echo "Hudi version must be specified using -h flag if package is not specified using the -k flag."
				return
			fi

			if [ "$HUDI_SPARK_VERSION" = "2.4" ]; then
				K_ARG="org.apache.hudi:hudi-spark2.4-bundle_2.11:$HUDI_VERSION_INPUT"
			else
				K_ARG="org.apache.hudi:hudi-spark${HUDI_SPARK_VERSION}-bundle_2.12:$HUDI_VERSION_INPUT"
			fi
		fi
		RUNNABLE_LOC="--packages $K_ARG"
	fi

	CONFS="--conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension --conf spark.serializer=org.apache.spark.serializer.KryoSerializer"
	EXTRA_CONFS="0"
	if [ "${HUDI_SPARK_VERSION:0:1}" -gt "3" ]; then
		EXTRA_CONFS="1"
	elif [ "${HUDI_SPARK_VERSION:0:1}" -eq "3" ] && [ "${HUDI_SPARK_VERSION:2:1}" -ge "2" ]; then
		EXTRA_CONFS="1"
	fi

	if [ "$EXTRA_CONFS" -eq "1" ]; then
		CONFS+=" --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog'"
	fi

	ALL_PARAMETERS="$RUNNABLE_LOC $CONFS $EXTRA_SPARK_CONFS"
	echo "ALL PARAMS: $ALL_PARAMETERS"
	case $SHELL_TYPE in
		spark)
			$SPARK_HOME/bin/spark-shell $ALL_PARAMETERS
			;;
		sql)
			$SPARK_HOME/bin/spark-sql $ALL_PARAMETERS
			;;
		pyspark)
			export PYSPARK_PYTHON=$(which python3)
			$SPARK_HOME/bin/pyspark $ALL_PARAMETERS
			;;
	esac
}

hudi_spark_shell() {
	run_hudi "$@ -q spark"
}

hudi_spark_sql() {
	run_hudi "$@ -q sql"
}

hudi_pyspark() {
	run_hudi "$@ -q pyspark"
}