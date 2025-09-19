#!/bin/bash

# Start the spark master in the background
/opt/spark/master.sh &

# Start the jupyterlab
/opt/spark/jupyter.sh

