#!/bin/bash

cd $(dirname "$0")
# go sub directories and launch the service
for dir in */; do
    if [ -d "$dir" ]; then
        echo "Launching service in $dir"

        # copy config file
        cp ./service-config.json "$dir/build/"

        cd "$dir/build"
        echo "./$(basename "$dir")"
        cd ../..
    fi
done
