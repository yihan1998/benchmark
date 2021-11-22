#!/bin/bash

dd if=/dev/urandom bs=1M count=128 | base64 > input.dat