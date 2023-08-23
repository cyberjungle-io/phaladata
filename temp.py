import json
import requests

# Get the RPC URL from the node
rpc_url = "http://10.2.4.1:30333"

# Get the last block number
last_block_number = requests.get(f"{rpc_url}/chains/phala/blocks/latest/header").json()["number"]

# Get the events from the last block
events = requests.get(f"{rpc_url}/chains/phala/blocks/{last_block_number}/events").json()

# Print the events
for event in events:
  print(event)
