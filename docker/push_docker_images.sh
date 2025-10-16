#!/bin/bash

TAGS=("latest" "4e6ebba26cb097cb26cddcbb3958d99dda476320")

IMAGE_NAMES=(
    "apachehudi/hudi-hadoop_3.3.4-base"
    "apachehudi/hudi-hadoop_3.3.4-namenode"
    "apachehudi/hudi-hadoop_3.3.4-datanode"
    "apachehudi/hudi-hadoop_3.3.4-history"
    "apachehudi/hudi-hadoop_3.3.4-hive_3.1.3"
    "apachehudi/hudi-hadoop_3.3.4-hive_3.1.3-sparkbase_3.5.3"
    "apachehudi/hudi-hadoop_3.3.4-hive_3.1.3-sparkmaster_3.5.3"
    "apachehudi/hudi-hadoop_3.3.4-hive_3.1.3-sparkworker_3.5.3"
    "apachehudi/hudi-hadoop_3.3.4-hive_3.1.3-sparkadhoc_3.5.3"
)

echo "Starting Docker push process for ${#IMAGE_NAMES[@]} images and ${#TAGS[@]} tags each"
echo "------------------------------------------------------------------------"

# Check if docker is installed
if ! command -v docker &> /dev/null
then
    echo "ERROR: Docker command not found. Please ensure Docker is installed and in your PATH."
    exit 1
fi

SUCCESS_COUNT=0
FAILURE_COUNT=0

for IMAGE in "${IMAGE_NAMES[@]}"; do
    for TAG in "${TAGS[@]}"; do
        FULL_IMAGE="${IMAGE}:${TAG}"

        echo "Attempting to push: ${FULL_IMAGE}"

        if ! docker image inspect "${FULL_IMAGE}" &> /dev/null; then
            echo "--> [SKIPPED] Image ${FULL_IMAGE} does not exist locally. Skipping push."
            FAILURE_COUNT=$((FAILURE_COUNT + 1))
            continue
        fi

        if docker push "${FULL_IMAGE}"; then
            echo "--> [SUCCESS] Successfully pushed ${FULL_IMAGE}"
            SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
        else
            echo "--> [FAILED] Failed to push ${FULL_IMAGE}. Check Docker login status and network."
            FAILURE_COUNT=$((FAILURE_COUNT + 1))
        fi
        
	echo "------------------------------------------------------------------------"
    done
done

echo "Docker Push Summary:"
echo "Total Pushes Attempted: $(( ${#IMAGE_NAMES[@]} * ${#TAGS[@]} ))"
echo "Successful: ${SUCCESS_COUNT}"
echo "Failed: ${FAILURE_COUNT}"

if [ "${FAILURE_COUNT}" -eq 0 ]; then
    echo "All images were successfully pushed."
else
    echo "Please review the logs above for failed images."
fi
