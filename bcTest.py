


from substrateinterface import SubstrateInterface, Keypair
from substrateinterface.exceptions import SubstrateRequestException

# import logging
# logging.basicConfig(level=logging.DEBUG)

try:
    substrate = SubstrateInterface(
        url="ws://10.2.4.1:9944",
        type_registry_preset="substrate-node-template"
    )
except ConnectionRefusedError:
    print("⚠️ No local Substrate node running, try running 'start_local_substrate_node.sh' first")
    exit()


""" account_info = substrate.query('PhalaComputation', 'ComputingHalvingInterval', [])
print('Halving Interval', account_info.value) 

account_info = substrate.query('PhalaComputation', 'ComputingStartBlock', [])
print('Starting Block', account_info.value) 

account_info = substrate.query('PhalaComputation', 'TokenomicParameters', [])
print('Tokenomic Parameters', account_info.value) 
 """
# account_info = substrate.query('System', 'PoolCount', [])
# print('Tokenomic Parameters', account_info.value) 


#account_info = substrate.query('PhalaComputation', 'Sessions', ["43E9fDc6HxfDaw5jhL7UGDX5acAPyRDxp2ig95jZ8saTuRSj"])
#print('Account info', account_info.value) 

# account_info = substrate.query('PhalaComputation', 'StakerAccounts', ["44RGVAd8sadC7Bitqe3tj5NTeXMrojE1NqGecdBiQX2bLUbG"])
# print('Account info', account_info.value) 

# account_info = substrate.query('System', 'Events', [])
# print('Account info', account_info.value)

account_info = substrate.query('PhalaBasePool', 'PoolContributionWhitelists', ['1674'])
print(account_info.value)

#result = substrate.query_map('System', 'Account')
""" 
for account, account_info in result:
    if account.value == "44RGVAd8sadC7Bitqe3tj5NTeXMrojE1NqGecdBiQX2bLUbG":
        print(f"Free balance of account '{account.value}': {account_info.value}")


        era_stakers = substrate.query_map(
    module='Staking',
    storage_function='ErasStakers',
    params=[2100]
)

print(era_stakers.value) """