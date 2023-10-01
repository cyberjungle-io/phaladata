from pymongo import MongoClient

def fetch_and_summarize_shares():
    # Connect to MongoDB
    client = MongoClient('mongodb://10.2.2.11:27017/')
    db = client['phala']
    collection = db['events']

    # Fetch documents with method="NftCreated" and pid=1673
    nft_created_cursor = collection.find({"method": "NftCreated", "pid": 1674})

    # Check for each NFT if it has been burned and summarize shares
    total_shares = 0
    for doc in nft_created_cursor:
        # Check if there's a corresponding "NFTBurned" record
        burned_cursor = collection.find_one({
            "method": "NFTBurned",
            "account_id": doc['account_id'],
            "cid": doc['cid'],
            "nft_id": doc['nft_id']
        })

        # If no corresponding "NFTBurned" record is found, add the shares
        if not burned_cursor:
            total_shares += doc.get('shares', 0)

    return total_shares


    

if __name__ == "__main__":
    total_shares = fetch_and_summarize_shares()
    print(f"Total shares for NFTs that haven't been burned: {total_shares}")

