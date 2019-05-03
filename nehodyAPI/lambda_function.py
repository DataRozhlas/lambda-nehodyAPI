import json
from boto3.dynamodb.conditions import Key, Attr
import base64
import io
import boto3
import operator
import time
from datetime import datetime, timedelta
from decimal import *
from template import gen_dict

s3 = boto3.client('s3')
dyn = boto3.resource('dynamodb', region_name='eu-central-1')
table = dyn.Table('nehody')

def default_week(tstamp):
    dt = datetime.utcfromtimestamp(tstamp)
    mon = dt - timedelta(days=dt.weekday())
    return int(time.mktime(mon.replace(hour=0, minute=0, second=0, microsecond=0).timetuple()))
    
def default_month(tstamp):
    dt = datetime.utcfromtimestamp(tstamp)
    mon = dt - timedelta(days=dt.day - 1)
    return int(time.mktime(mon.replace(hour=0, minute=0, second=0, microsecond=0).timetuple()))

def default_year(tstamp):
    dt = datetime.utcfromtimestamp(tstamp)
    mon = dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    return int(time.mktime(mon.timetuple()))

def lambda_handler(event, context):
    e = eval(base64.b64decode(event['q']))
    resp = table.query(
        KeyConditionExpression=Key('primkey').eq(1) & Key('tstamp').between(e['from'], e['to'])
    )
    out = {}
    
    dates = []
    scope = 'all'
    if e['all'] == 'true':
        cr = {}
        out = gen_dict()
        for i in resp['Items']:
            dates.append(i['tstamp'])
            for kraj in i['data']:
                for val in i['data'][kraj]:
                    out[kraj][val] += i['data'][kraj][val]
        cr = {
            "JP": 0,
            "LR": 0,
            "M": 0,
            "NP": 0,
            "NPJ": 0,
            "NR": 0,
            "NZJ": 0,
            "PN": 0,
            "PVA": 0,
            "TR": 0,
            "Š": 0
        }
        for kraj in out:
            for key in out[kraj]:
                cr[key] += out[kraj][key]
        out['ČR'] = cr
    elif resp['Count'] <= 31:
        scope = 'days'
        # pod dnech
        for i in resp['Items']:
            dates.append(i['tstamp'])
            out[str(i['tstamp'])] = i['data']
    elif resp['Count'] <= (31 * 6):
        # po týdnech
        scope = 'weeks'
        for i in resp['Items']:
            dates.append(i['tstamp'])
            wstamp = default_week(i['tstamp']) #week timestamp
            if wstamp not in out:
                out[wstamp] = gen_dict()
            for kraj in i['data']:
                for val in i['data'][kraj]:
                    out[wstamp][kraj][val] += i['data'][kraj][val]
    elif resp['Count'] <= 720:
        # po měsících
        scope = 'months'
        for i in resp['Items']:
            dates.append(i['tstamp'])
            mstamp = default_month(i['tstamp']) #month timestamp
            if mstamp not in out:
                out[mstamp] = gen_dict()
            for kraj in i['data']:
                for val in i['data'][kraj]:
                    out[mstamp][kraj][val] += i['data'][kraj][val]
    else:
        # po letech
        scope = 'years'
        for i in resp['Items']:
            dates.append(i['tstamp'])
            mstamp = default_year(i['tstamp']) # year timestamp
            if mstamp not in out:
                out[mstamp] = gen_dict()
            for kraj in i['data']:
                for val in i['data'][kraj]:
                    out[mstamp][kraj][val] += i['data'][kraj][val]
                    
    # secist cr pro useky
    if e['all'] != 'true':
        for seq in out:
            cr = {
                "JP": 0,
                "LR": 0,
                "M": 0,
                "NP": 0,
                "NPJ": 0,
                "NR": 0,
                "NZJ": 0,
                "PN": 0,
                "PVA": 0,
                "TR": 0,
                "Š": 0
            }
            for kraj in out[seq]:
                for key in out[seq][kraj]:
                    cr[key] += out[seq][kraj][key]
            out[seq]['ČR'] = cr
    
    if len(dates) > 0: # pro pripad, ze to nevrati nic
        out.update({
            'scope': scope,
            'bounds': {
                'from': min(dates),
                'to': max(dates)
            }
        })
    else:
        out.update({
            'scope': scope, 
            'bounds': {
                'from': 0,
                'to': 0
            }
        })
    
    return out