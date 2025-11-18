#!/bin/bash

cd $(dirname "$0")
# go sub directories and launch the service
for dir in */; do
    if [ -d "$dir" ]; then
        echo "Launching service in $dir"

        # copy config file
        # mkdir -p "$dir/build/config"
        # cp ./service-config.json "$dir/build/config/service-config.json"

        cd "$dir/build"
        ./$(basename "$dir") &
        cd ../..
    fi
done
