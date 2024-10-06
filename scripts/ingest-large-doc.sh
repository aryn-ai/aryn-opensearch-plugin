#!/usr/bin/env sh

attached_file="$1"
index_name="$2"
doc_id="$3"

HOST="localhost:9200"

# Pipe the base64 encoded content of attached_file
base64 --wrap=0 "$attached_file" |
# into jq to make it a proper JSON string within the
# JSON data structure
jq --slurp --raw-input --arg FileName "$attached_file" \
'{
   "data": . 
}
' |
# Get the resultant JSON piped into curl
# that will read the data from the standard input
# using -d @-
curl -X POST -d @- \
   $HOST'/'$index_name'/_doc/'$doc_id  \
   -H 'content-type: application/json'
