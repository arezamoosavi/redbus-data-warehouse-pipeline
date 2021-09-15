#!/bin/bash

set -o errexit
set -o nounset

echo ". . . . . Faust Is RUNNING! . . . . ."

faust -A pie_app worker -l info -p 6066