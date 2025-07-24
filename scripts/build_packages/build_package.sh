#!/bin/bash
set -e

# Parse command line arguments
BUILD_ALL=${1:-false}

echo "Building Python package..."
echo "Build all packages: $BUILD_ALL"

rm -rf ./build
rm -rf ./dist
rm -rf ./venv
rm -rf ./*.egg-info


pipenv run python -m venv venv

source ./venv/bin/activate

pip install build

# build native packages
echo "Building native packages..."
python -m build # wheel and tar.gz
python setup.py bdist_egg # egg file

if [ "$BUILD_ALL" = "true" ]; then
    echo "Building third-party dependencies..."

    pip install -r requirements.txt
    
    pip install venv-pack pex
    
    venv-pack -o ./dist/venv.tar.gz # 3rd party dependencies
    
    pex -r requirements.txt -o ./dist/env.pex # 3rd party dependencies + python environment
fi


echo "Package build completed successfully!"
