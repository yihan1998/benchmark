#!/bin/bash

dd if=/dev/urandom bs=1M count=512 | base64 > input.dat