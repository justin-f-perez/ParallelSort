#!/usr/bin/env bash
set -e           # fail/exit if any command in the script yields an exit error code
set -u           # fail/exit if attempting to expand an env var that isn't set
set -o pipefail  # fail/exit if a pipeline fails

#DISABLED: too noisy, outputs all the stuff inside of the virtual environment activate script too
#set -x           # print commands to stdout as they run  


cd `dirname $0`  # change into the directory of this script just incase it was invoked from outside of it
echo "working directory: `pwd`"
echo "Creating a python virtual environment to install dependencies into..."
python3 -m venv .venv
echo "Activating virtual environment so 'pip install' commands will run inside the virtual environment..."
source .venv/bin/activate
echo "Installing dependencies (this may take a few minutes)"
pip install -r requirements.txt
