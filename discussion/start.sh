#!/usr/bin/env bash

cd `dirname $0`  # change into the directory of this script just incase it was invoked from outside of it
echo "working directory: `pwd`"
echo "Activating virtual environment where python dependencies are installed..."
source .venv/bin/activate
jupyter lab
