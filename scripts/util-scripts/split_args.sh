#!/bin/bash

# Tool to split args and getting them ready for YAML format

transformed_args=""

# Loop through all arguments (starting from the second) 
for arg in "$@"
do
  # Check format --xx=yy
  if [[ $arg =~ ^--(.+)=(.+)$ ]]; then
    option="${BASH_REMATCH[1]}"
    value="${BASH_REMATCH[2]}"

    transformed_args="$transformed_args,\"--$option\",\"$value\""
  else
    # Space format
    transformed_args="$transformed_args,\"$arg\"" 
  fi
done

echo "${transformed_args:1}"
