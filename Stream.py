#!/usr/bin/env python3
import boto3
import json
import os
import oandapyV20
import oandapyV20.endpoints.pricing as pricing
# from oandapyV20.exceptions import StreamTerminated
import yaml


def connect_aws(config_filepath='aws_cred.yml'):
    """return firehose boto3 client
    """
    awscred = yaml.load(open(os.path.expanduser(config_filepath)))
    aws = boto3.client('firehose',
                       region_name='us-east-1', **awscred['AWS'])
    return aws


def connect_oanda(config_filepath="oanda_cred.yml",
                  instruments=["USD_JPY", "GBP_JPY", "EUR_USD", "EUR_GBP"]):
    cred = yaml.load(open(os.path.expanduser(config_filepath)))
    access_token = cred['oanda']['token']
    accountID = cred['oanda']['id']

    return oandapyV20.API(access_token=access_token), pricing.PricingStream(
            accountID=accountID, params={"instruments": ",".join(instruments)})


def main():
    aws = connect_aws()
    client, r = connect_oanda()

    while True:
        # aws = connect_aws()
        # client, r = connect_oanda()
        for tick in client.request(r):
            data = json.dumps(tick) + '\n'
            aws.put_record(DeliveryStreamName='FX',
                           Record={'Data': data})


if __name__ == '__main__':
    main()
