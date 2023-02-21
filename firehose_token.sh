#!/bin/sh

# load environment vairables from .env file
if [ -f .env ]
then
  export $(grep -v '^#' .env | xargs)
fi

# get API token for substreams
export SUBSTREAMS_API_TOKEN=$(curl https://auth.streamingfast.io/v1/auth/issue -s --data-binary '{"api_key":"'$STREAMINGFAST_KEY'"}' | jq -r .token)
