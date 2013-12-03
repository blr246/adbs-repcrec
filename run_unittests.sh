#!/bin/bash

pushd tests 1>/dev/null

for file in *.py; do
    echo Running tests from ${file}...
    test_module="${file%.*}"
    python -m unittest ${test_module}
done

popd 1>/dev/null
