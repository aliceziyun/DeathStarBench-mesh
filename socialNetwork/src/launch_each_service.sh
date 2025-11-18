#!/bin/bash

cd $(dirname "$0")
# go sub directories and launch the service
for dir in */; do
    if [ -d "$dir" ]; then
        echo "Compiling service in $dir"

        mkdir -p "$dir/build"
        cd "$dir/build"
        cmake ..
        make -j4
        cd ../..
    fi
done
