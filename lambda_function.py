import requests
import boto3
import json
from datetime import datetime
import logging
import time

def lambda_handler(event, context):
    
    # Parse the event JSON string to a dictionary
    event_dict = json.loads(json.dumps(event))

    # Read the input parameters from the event dictionary
    exchange = event_dict['exchange']
    level = event_dict['level']
    market = event_dict['market']
    bucket_name = event_dict['bucket_name']
    bucket_prefix = event_dict['bucket_prefix']
    maxAmount = event_dict['max_amount_sum']
    writeCSVHeaders = event_dict['write_csv_headers']
    
    # Make a GET request REST API
    if exchange == 'Coinbase':
        marketUSD = market + '-USD'
        url = "https://api.pro.coinbase.com/products/" + marketUSD + "/book?level=" + str(level)
    elif exchange == 'Kraken':
        marketUSD = market + 'USD'
        url = "https://api.kraken.com/0/public/Depth?pair=" + marketUSD
    
    response = requests.get(url)
    data = response.json()
    
    logging.info('data: ' + str(data))

    current_time = datetime.now()
    
    # Both the Coinbase and Kraken API documentation for the book endpoint states that the order book entries are already sorted by price,
    # from highest to lowest for bids and from lowest to highest for asks, but let's sort them anyway to make sure
    
    # Extract the bid and ask data from the response
    if exchange == 'Coinbase':
        bids = data['bids']
        asks = data['asks']
    elif exchange == 'Kraken':
        marketTemp = ""
        # These don't correspond 1:1 to the parameter that is passed to the API,
        # Need to investigate if there is a mapping that I can use to not hardcore it
        if market == 'BTC':
            marketTemp = 'XXBTZUSD' 
        elif market == 'ETH':
            marketTemp = 'XETHZUSD'
        
        bids = data["result"][marketTemp]["bids"]
        asks = data["result"][marketTemp]["asks"]
    
    # Sort bids in descending order by price
    sorted_bids = sorted(bids, key=lambda x: float(x[0]), reverse=True)
    # Sort asks in ascending order by price
    sorted_asks = sorted(asks, key=lambda x: float(x[0]))

    # Create a new dictionary with the price and size keys for each bid
    bids = [{'price': bid[0], 'size': bid[1]} for bid in sorted_bids]

    # Create a new dictionary with the price and size keys for each ask
    asks = [{'price': ask[0], 'size': ask[1]} for ask in sorted_asks]
    
    amountBid = 0.0
    amountAsk = 0.0
    bidCount = 0
    askCount = 0
    totalBidPrice = 0.0
    totalAskPrice = 0.0 

    # Convert the JSON data to CSV format
    header = ['exchange', 'time', 'market', 'type', 'price', 'size']
    rows = []
    for bid in sorted_bids:
        if amountBid < maxAmount:
            rows.append([exchange, str(current_time), market, 'bid', bid[0], bid[1]])
            thisBid = float(bid[0]) * float(bid[1])
            amountBid = amountBid + thisBid
            totalBidPrice = totalBidPrice + float(bid[0])
            bidCount = bidCount + 1
        else:
            break
            
    for ask in sorted_asks:
        if amountAsk < maxAmount:
            rows.append([exchange, str(current_time), market, 'ask', ask[0], ask[1]])
            thisAsk = float(ask[0]) * float(ask[1])
            amountAsk = amountAsk + thisAsk
            totalAskPrice = totalAskPrice + float(ask[0])
            askCount = askCount + 1
        else:
            break
 
    # Used only for the Timestream option
    midPrice = ((totalBidPrice / bidCount) + (totalAskPrice / askCount)) / 2

    csvData = ""

    # Athena is also looking at the column names in the CSV, need to see if there is a setting to ignore that first row
    # For now let's not put the header
    if writeCSVHeaders == 1:
        csv_data = '\n'.join([','.join(row) for row in [header] + rows])
    else:
        csv_data = '\n'.join([','.join(row) for row in rows])

    # Create a new S3 client
    s3 = boto3.client('s3')
    key = bucket_prefix + '/' + exchange + '/' + market + '/' + exchange + '_' + market + '_' + str(current_time) + '_.csv'

    # Upload the data to S3
    s3.put_object(Bucket=bucket_name, Key=key, Body=csv_data.encode('utf-8'))
    
    # Buid the mid price timestream record
    records = []
    midPriceRecord = {
        'Dimensions': [
            {
                'Name': 'Exchange', 'Value': exchange
            },
            {
                'Name': 'Market', 'Value': market
            }
        ],
        'MeasureName': 'mid_price',
        'MeasureValue': str(midPrice),
        'MeasureValueType': 'DOUBLE',
        'Time': str(int(time.time() * 1000))
    }
    records.append(midPriceRecord)
    
    # Define the Timestream client
    timestream = boto3.client('timestream-write')
    
    # Write the record to Timestream
    timestream.write_records(DatabaseName='CrytoExchangeData', TableName='orderbookdata', Records=records)

    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Data ingested successfully to S3, response: ' + str(response)})
    }