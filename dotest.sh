#!/bin/bash
set -x
set -e

logfile=~/temp/rlog

go test -v -race -run $@ |& tee ${logfile}

go run ./tool.go < ${logfile}
