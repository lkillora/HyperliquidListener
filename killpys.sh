#!/bin/bash

# List of Python script names to search for (you can add more names to this list)
script_names=("liquidity.py" "positions.py" "summary_stats.py" "prices.py" "hydromancer_ws_filters.py")

# Loop through each script name and kill the processes
for script_name in "${script_names[@]}"; do
    # Get process IDs of the Python scripts that match
    pids=$(pgrep -f "python.*$script_name")

    # If processes are found, kill them
    if [ ! -z "$pids" ]; then
        echo "Killing processes running $script_name"
        kill $pids
    else
        echo "No processes found running $script_name"
    fi
done