from web3 import Web3
from web3.contract import Contract
from web3.providers.rpc import HTTPProvider
from web3.middleware import geth_poa_middleware #Necessary for POA chains
import json
from datetime import datetime
import pandas as pd
import os

eventfile = 'deposit_logs.csv'

def scanBlocks(chain, start_block, end_block, contract_address):
    """
    chain - string (Either 'bsc' or 'avax')
    start_block - integer first block to scan
    end_block - integer last block to scan
    contract_address - the address of the deployed contract
    This function reads "Deposit" events from the specified contract, 
    and writes information about the events to the file "deposit_logs.csv"
    """
    if chain == 'avax':
        api_url = f"https://api.avax-test.network/ext/bc/C/rpc" #AVAX C-chain testnet
    if chain == 'bsc':
        api_url = f"https://data-seed-prebsc-1-s1.binance.org:8545/" #BSC testnet
    
    if chain in ['avax','bsc']:
        w3 = Web3(Web3.HTTPProvider(api_url))
        # inject the poa compatibility middleware to the innermost layer
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    else:
        w3 = Web3(Web3.HTTPProvider(api_url))
    
    DEPOSIT_ABI = json.loads('[ { "anonymous": false, "inputs": [ { "indexed": true, "internalType": "address", "name": "token", "type": "address" }, { "indexed": true, "internalType": "address", "name": "recipient", "type": "address" }, { "indexed": false, "internalType": "uint256", "name": "amount", "type": "uint256" } ], "name": "Deposit", "type": "event" }]')
    contract = w3.eth.contract(address=contract_address, abi=DEPOSIT_ABI)
    arg_filter = {}
    
    if start_block == "latest":
        start_block = w3.eth.get_block_number()
    if end_block == "latest":
        end_block = w3.eth.get_block_number()
        
    if end_block < start_block:
        print(f"Error end_block < start_block!")
        print(f"end_block = {end_block}")
        print(f"start_block = {start_block}")
        return
        
    if start_block == end_block:
        print(f"Scanning block {start_block} on {chain}")
    else:
        print(f"Scanning blocks {start_block} - {end_block} on {chain}")

    # Initialize list to store all events
    all_events_data = []
    
    def process_events(events):
        for evt in events:
            event_data = {
                'chain': chain,
                'token': evt.args['token'],  # Don't convert to hex yet
                'recipient': evt.args['recipient'],  # Don't convert to hex yet
                'amount': evt.args['amount'],
                'transactionHash': evt.transactionHash,  # Don't convert to hex yet
                'address': contract_address  # Keep original format
            }
            all_events_data.append(event_data)
    
    if end_block - start_block < 30:
        event_filter = contract.events.Deposit.create_filter(
            fromBlock=start_block,
            toBlock=end_block,
            argument_filters=arg_filter
        )
        events = event_filter.get_all_entries()
        process_events(events)
    else:
        for block_num in range(start_block, end_block + 1):
            event_filter = contract.events.Deposit.create_filter(
                fromBlock=block_num,
                toBlock=block_num,
                argument_filters=arg_filter
            )
            events = event_filter.get_all_entries()
            process_events(events)
    
    # Create DataFrame and format the data
    if all_events_data:
        df = pd.DataFrame(all_events_data)
        
        # Convert addresses and hashes to proper hex format (without '0x' prefix)
        for col in ['token', 'recipient', 'transactionHash', 'address']:
            df[col] = df[col].apply(lambda x: x.hex() if isinstance(x, bytes) else x)
        
        # Define the exact column order
        columns = ['chain', 'token', 'recipient', 'amount', 'transactionHash', 'address']
        df = df[columns]
        
        # Write to CSV with specific formatting
        if not os.path.exists(eventfile):
            # First time writing - include headers
            df.to_csv(eventfile, index=False, header=True)
        else:
            # Append without headers
            df.to_csv(eventfile, mode='a', index=False, header=False)