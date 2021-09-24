set -e           # fail/exit if any command in the script yields an exit error code
set -u           # fail/exit if attempting to expand an env var that isn't set
set -o pipefail  # fail/exit if a pipeline fails
set -x           # print commands to stdout as they run

# clears output of python notebooks on commit to prevent noisey diffs
cp pre-commit ../.git/hooks/

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -qq
