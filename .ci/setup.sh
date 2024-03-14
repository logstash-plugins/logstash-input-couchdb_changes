#!/bin/bash

if [ $(command -v apt) ]; then
    sudo apt install -y make
else
    sudo yum install -y make
fi
