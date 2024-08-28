#!/bin/bash

curl -H "Authorization: Token 5689361998b15b52861c5933ef527ac1ede80d19" -X POST -H 'Content-type:application/json' -d @data/test.json "https://searchcloud-2-us-east-1.searchstax.com/29847/kmw-1804/update"
