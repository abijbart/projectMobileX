#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written for Python 3.7.3

Creates NetworkX graph of Milan SMS/Phone/Internet connections to countries
(organized by phone calling codes)

Creates associated .json files as well
"""


from itertools import chain
import json
from pathlib import Path
import pickle
import os

import networkx


# If the raw data doesn't live with this Python script, change this to match
# where the data does live on the system
DATA_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
CELL_MAPPING_PATH = Path(DATA_DIR.parent, 'MilanCensusMapping', 'milan_grid_census_codes_map.json')
JSON_PATH = Path(DATA_DIR, 'sms-call-internet-mi.json')
CALL_PICKLE_PATH = Path(DATA_DIR, 'call-mi.pickle')
INTERNET_PICKLE_PATH = Path(DATA_DIR, 'internet-mi.pickle')
SMS_PICKLE_PATH = Path(DATA_DIR, 'sms-mi.pickle')
COUNTRY_ABBR_PATH = Path(DATA_DIR, 'names.json')
PHONE_ABBR_PATH = Path(DATA_DIR, 'phone.json')
CODES_MAP_PATH = Path(DATA_DIR, 'country_codes_map.json')


# Is string an integer? If yes, return string as integer.
def is_int(s):

    if s == '':
        return s

    try:
        s = int(s)
        return s
    except ValueError:
        return False


# Is string a float? If yes, return string as float. 
def is_float(s):

    if s == '':
        return s

    try:
        s = float(s)
        return s
    except ValueError:
        return False


# See description within collect_json() for more information on what validation
# is occurring
def validate(tokens):

    if len(tokens) != 8:
        raise ValueError(f'tokens length is not 8: {len(tokens)}')

    for i, token in enumerate(tokens):

        # Don't validate the timestamp. It's unused.
        if i == 1:
            continue

        if i < 3:
            tokens[i] = is_int(token)
            if tokens[i] != 0 and tokens[i] == False:
                raise ValueError(f'tokens[{i}] is not an int: {tokens[i]}')

        else:

            tokens[i] = is_float(token)
            if tokens[i] != 0 and tokens[i] == False:
                raise ValueError(f'tokens[{i}] is not a float: {tokens[i]}')

    return tokens


def collect_json():

    if not DATA_DIR.is_dir():
        raise FileNotFoundError(DATA_DIR)

    graph = {}
    for filepath in DATA_DIR.iterdir():

        if filepath.suffix != '.txt':
            continue

        # Sanity print
        print(filepath)

        with open(filepath, 'r') as stream:

            # Lines are tab separated
            # Only fields 0-2 are guaranteed. All others are optional
            # [0] is Milan cell id
            # [1] is the timestamp that begins the 10-minute collection interval
            # [2] is the country code of the relationship.
            #     0 is frequent, but not a valid country code. It made it into
            #     the graph because it's so frequent. If that proves to be
            #     spurious data, we can take it out of the graph later
            # [3] SMS-in activity
            # [4] SMS-out activity
            # [5] Call-in activity
            # [6] Call-out activity
            # [7] "Number of CDRs generated inside a given Square id during a
            #      given Time interval. The Internet traffic is initiated
            #      from the nation identified by the Country code"
            #      This field is included, but it's usefulness is questionable.
            #      Internet users seek out websites and web services.
            #      The physical location of the servers hosting that content is
            #      more likely to be related to laws and geographic proximity
            #      than anything else.
            #      Values are floats.
            for line in stream:

                tokens = validate(line.split('\t'))
                if tokens[0] not in graph:
                    graph[tokens[0]] = {}
                local = graph[tokens[0]]
                if tokens[2] not in local:
                    local[tokens[2]] = {
                        'smsIn': 0,
                        'smsInCount': 0,
                        'smsOut': 0,
                        'smsOutCount': 0,
                        'callIn': 0,
                        'callInCount': 0,
                        'callOut': 0,
                        'callOutCount': 0,
                        'cdr': 0,
                    }
                local = local[tokens[2]]
                if tokens[3]:
                    local['smsIn'] += tokens[3]
                    local['smsInCount'] += 1
                if tokens[4]:
                    local['smsOut'] += tokens[4]
                    local['smsOutCount'] += 1
                if tokens[5]:
                    local['callIn'] += tokens[5]
                    local['callInCount'] += 1
                if tokens[6]:
                    local['callOut'] += tokens[6]
                    local['callOutCount'] += 1
                if tokens[7]:
                    local['cdr'] += tokens[7]

        # Write after each file in-case we run out of memory
        with open(JSON_PATH, 'w') as stream:
            json.dump(graph, stream)


def json_to_networkx():

    if not DATA_DIR.is_dir():
        raise FileNotFoundError(DATA_DIR)

    if not JSON_PATH.is_file():
        raise FileNotFoundError(JSON_PATH)

    graphDict = None
    graphCall = networkx.Graph()
    graphSMS = networkx.Graph()
    graphInternet = networkx.Graph()

    with open(JSON_PATH, 'r') as stream:
        graphDict = json.load(stream)

    for node1, innerDict in graphDict.items():

        node1 = int(node1)

        # Both node1 and node2 are integer values
        # But node1 represent Milan cell IDs
        # And node2 represents country calling codes
        node1 = 'm' + str(node1)
        graphCall.add_node(node1)
        graphInternet.add_node(node1)
        graphSMS.add_node(node1)

        for node2, connectDict in innerDict.items():

            # Both node1 and node2 are integer values
            # But node1 represent Milan cell IDs
            # And node2 represents country calling codes
            node2 = 'c' + node2
            if node2 not in graphCall.nodes:
                graphCall.add_node(node2)
            if node2 not in graphInternet.nodes:
                graphInternet.add_node(node2)
            if node2 not in graphSMS.nodes:
                graphSMS.add_node(node2)

            callWeight = 0
            smsWeight = 0

            # Check before dividing by zero
            #
            # Normalize by adjusting by the number of time periods the
            # connection appears in
            # We lack absolute counts, only relative frequency with time
            # periods, so this is the best normalization available
            if connectDict['callInCount'] > 0:
                callWeight += connectDict['callIn'] / connectDict['callInCount']
            if connectDict['callOutCount'] > 0:
                callWeight += connectDict['callOut'] / connectDict['callOutCount']
            if connectDict['smsInCount'] > 0:
                smsWeight += connectDict['smsIn'] / connectDict['smsInCount']
            if connectDict['smsOutCount'] > 0:
                smsWeight += connectDict['smsOut'] / connectDict['smsOutCount']

            try:
                graphCall.edges[node1, node2]['weight'] += callWeight
            except KeyError:
                graphCall.add_edge(node1, node2, weight=callWeight)
            try:
                graphSMS.edges[node1, node2]['weight'] += smsWeight
            except KeyError:
                graphSMS.add_edge(node1, node2, weight=smsWeight)
            try:
                graphInternet.edges[node1, node2]['weight'] += connectDict['cdr']
            except KeyError:
                graphInternet.add_edge(node1, node2, weight=connectDict['cdr'])


    with open(CALL_PICKLE_PATH, 'wb') as stream:
        pickle.dump(graphCall, stream)
    with open(INTERNET_PICKLE_PATH, 'wb') as stream:
        pickle.dump(graphInternet, stream)
    with open(SMS_PICKLE_PATH, 'wb') as stream:
        pickle.dump(graphSMS, stream)


def create_country_code_map():

    if not DATA_DIR.is_dir():
        raise FileNotFoundError(DATA_DIR)

    abbrToCodes = {}
    abbrToNames = {}
    codesToAbbr = {}
    codes = set()
    graphCall = load_graph(CALL_PICKLE_PATH)
    graphInternet = load_graph(INTERNET_PICKLE_PATH)
    graphSms = load_graph(SMS_PICKLE_PATH)
    
    # The country codes begin with 'c'
    for node in chain(graphCall.nodes, graphInternet.nodes, graphSms.nodes):
        if node.startswith('c'):
            codes.add(node[1:])

    with open(COUNTRY_ABBR_PATH, 'r') as stream:
        abbrToNames = json.load(stream)
    with open(PHONE_ABBR_PATH, 'r') as stream:
        abbrToCodes = json.load(stream)
    for key, val in abbrToCodes.items():

        # The codes in the PHONE_ABBR_PATH json are more traditional, including
        # "+", "-", and multiple codes per country
        # In the Milan data, however, they are only digits.
        # This strips the non-digits and splits the codes on spaces
        for code in ''.join(list(
            filter(lambda x: x in ' 0123456789', val)
        )).split(' '):
            # The split can result in empty strings
            # Ignore the empty strings
            if code:
                codesToAbbr[code] = key

    countryCodeMap = {}
    for code in codes:
        try:
            countryCodeMap[code] = abbrToNames[codesToAbbr[code]]
        except KeyError:
            # This try was to be safe and never lose a code that happened to go
            # to a non-existent country name, but that never occurred.
            # Thus the resulting dictionary always had names or None
            try:
                countryCodeMap[code] = codesToAbbr[code]
            except KeyError:
                countryCodeMap[code] = None

    # Sanity print
    # Data is small enough that a print is helpful output
    print(countryCodeMap)
    with open(CODES_MAP_PATH, 'w') as stream:
        json.dump(countryCodeMap, stream)


def load_graph(path):

    graph = None
    with open(path, 'rb') as stream:
        graph = pickle.load(stream)

    return graph


if __name__ == "__main__":
    #collect_json()
    #json_to_networkx()
    #create_country_code_map()
