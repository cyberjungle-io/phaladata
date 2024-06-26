from pymongo import MongoClient

def calculate_shares_and_rewards(pid):
    # Connect to MongoDB
    client = MongoClient("mongodb://10.2.2.11:27017")
    db = client["phala"]
    collection = db["events"]

    # Step 1: Query to get the highest timestamp for each account_id
    pipeline = [
        {"$match": {"method": "NftCreated", "pid": pid}},
        {
            "$group": {
                "_id": "$account_id",
                "max_timestamp": {"$max": "$timestamp"}
            }
        }
    ]

    highest_timestamps = list(collection.aggregate(pipeline))

    withdrawal = 0
    # Print account_id and timestamp
    for record in highest_timestamps:
        print(record["_id"], record["max_timestamp"], withdrawal)

    withdrawal_cursor = collection.find({
            "method": "Withdrawal",
            "pid": pid
            
        }).sort("timestamp", -1).limit(1)
    for w in withdrawal_cursor:
        withdrawal += w["shares"]
    # Dictionary to store the shares for each account_id
    account_shares_lowest = {}
    account_shares_highest = {}

    # Iterate through the highest timestamps for each account_id
    for record in highest_timestamps:
        # For lowest NFT
        account_cursor_lowest = collection.find({
            "method": "NftCreated",
            "pid": pid,
            "account_id": record["_id"],
            "timestamp": record["max_timestamp"]
        }).sort("nft_id", 1)
        first_record_lowest = next(account_cursor_lowest, None)
        if first_record_lowest:
            account_shares_lowest[first_record_lowest["account_id"]] = first_record_lowest["shares"]

        # For highest NFT
        account_cursor_highest = collection.find({
            "method": "NftCreated",
            "pid": pid,
            "account_id": record["_id"],
            "timestamp": record["max_timestamp"]
        }).sort("nft_id", -1)
        first_record_highest = next(account_cursor_highest, None)
        if first_record_highest:
            account_shares_highest[first_record_highest["account_id"]] = first_record_highest["shares"]

    total_shares_lowest = sum(account_shares_lowest.values())
    print("Total Sum of Shares from the first NFT:", total_shares_lowest)

    total_shares_highest = sum(account_shares_highest.values()) - withdrawal
    print("Total Sum of Shares from the highest NFT:", total_shares_highest)

    # Get the last Contribution
    contribution_cursor = collection.find({
        "method": {"$in": ["Withdrawal", "Contribution"]},
        "pid": pid,
    }).sort("timestamp", -1).limit(1)

    lastSharePrice = 1
    lastTimestamp = 0
    if contribution_cursor:
        lastSharePrice = contribution_cursor[0]["amount"] / contribution_cursor[0]["shares"]
        lastTimestamp = contribution_cursor[0]["timestamp"]

    # Total Rewards
    reward_cursor = collection.find({
        "method": "RewardReceived",
        "pid": pid,
        "timestamp": {"$gt": lastTimestamp},
    })
    totalReward = 0
    for r in reward_cursor:
        totalReward += r["to_staker"]

    print("Total Rewards from last contributions:", totalReward)

    # Calculate shares
    sharePrice = (((totalReward + total_shares_highest)/total_shares_highest)-1)+lastSharePrice

    print("Share Price:", sharePrice)
    print("Total Amount:", sharePrice * total_shares_highest)
    
    return {
        "Total Sum of Shares from the first NFT": total_shares_lowest,
        "Total Sum of Shares from the highest NFT": total_shares_highest,
        "Total Rewards from last contributions": totalReward,
        "Share Price": sharePrice,
        "Total Amount": sharePrice * total_shares_highest
    }

#Example usage:
result = calculate_shares_and_rewards(454)
print(result)
