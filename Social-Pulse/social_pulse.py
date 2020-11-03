#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written for Python 3.7.3

Creates maps between Milan cells and census blocks to tweets within those
geographic regions. Some cleaning of the tweet metadata also occurs.
"""


from collections import defaultdict
import json
import os
from pathlib import Path
import urllib

import geojson
from shapely.geometry import Polygon, Point


# Local data paths
DATA_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
MILAN_GRID_PATH = Path(DATA_DIR.parent, 'Milan-Census-Mapping', 'milano-grid.geojson')
TWITTER_GEOJSON_PATH = Path(DATA_DIR, 'social-pulse-milano.geojson')
CELL_TWEETS_PATH = Path(DATA_DIR, 'cell_tweets.json')
CENSUS_GEOJSON_DIR = Path(DATA_DIR.parent, 'Milan-Census-Mapping', 'CensusGeojson')
CENSUS_POLYGONS_PATH = Path(DATA_DIR.parent, 'Milan-Census-Mapping', 'census_polygons.json')
CENSUS_BLOCKS_TWEETS_PATH = Path(DATA_DIR, 'census_blocks_tweets.json')


def get_milan_polygons():
"""
Get a dictionary of Milan cells (key cell ID) to their respective polygons
"""

    milanPolygons = {}
    with open(MILAN_GRID_PATH, 'r') as stream:
        geojsonDump = geojson.load(stream)

        for feature in geojsonDump['features']:

            if feature['geometry']['type'] == 'Polygon':

                # For some reason there's an extra list on the cooridnates
                # That messes up the contructor of Polygon
                milanPolygons[feature['properties']['cellId']] = Polygon(
                    feature['geometry']['coordinates'][0]
                )

    return milanPolygons


def get_cells_tweets_map():
    """
    Write out a JSON map of Milan cells (key is cell ID) to a list of 
    tweets from given Milan cell 
    """


    milanPolygons = get_milan_polygons()
    cells = defaultdict(list)

    with open(TWITTER_GEOJSON_PATH, 'r') as stream:
        geojsons = geojson.load(stream)['features']

    for tweet in geojsons:

        coords = tweet['geomPoint.geom']['coordinates']
        point = Point(coords[0], coords[1])

        # Brute force naive search across all cells
        # Takes ~1 hour, but it would take more than an hour to write code that
        # wasn't so naive.
        for cellID, polygon in milanPolygons.items():
            if polygon.contains(point):

                cells[cellID].append(create_tweet_dict(coords, tweet))
                # We don't care if a tweet borders two cells. Just stuff it
                # into one and quit searching.
                # Bordering on both is highly unlikely.
                break


    with open(CELL_TWEETS_PATH, 'w') as stream:
        json.dump(dict(cells), stream)


def get_census_tweets_map():
    """
    Write out a JSON map of census blocks (key SEZ2011) to a list of 
    tweets from given census blocks
    """

    blocks = defaultdict(list)
    censusPolygons = []
    geojsons = {}

    # Census geojson data
    with open(CENSUS_POLYGONS_PATH, 'r') as stream:
        censusPolygons = json.load(stream)

    # Polygon is a a raw list of floats for serialization.
    # We want a Polygon object
    for polygon in censusPolygons:
        polygon['polygon'] = Polygon(polygon['polygon'])

    # Tweet geojson
    with open(TWITTER_GEOJSON_PATH, 'r') as stream:
        geojsons = geojson.load(stream)['features']

    for tweet in geojsons:

        coords = tweet['geomPoint.geom']['coordinates']
        point = Point(coords[0], coords[1])

        # Brute force naive search across all blocks.
        # Takes about 45 minutes
        for block in censusPolygons:
            if block['polygon'].contains(point):

                blocks[block['SEZ2011']].append(create_tweet_dict(coords, tweet))
                # We don't care if a tweet borders two block. Just stuff it
                # into one and quit searching.
                # Bordering on both is highly unlikely.
                break


    with open(CENSUS_BLOCKS_TWEETS_PATH, 'w') as stream:
        # defaultdict may be JSON serializable, but I'm not 100% sure.
        # Convert to dict in case.
        json.dump(dict(blocks), stream)


def prune_cell_tweets():
    """
    Remove cells from CELL_TWEETS_PATH that do not have any tweets
    """

    cells = {}
    with open(CELL_TWEETS_PATH, 'r') as stream:
        cells = json.load(stream)

    with open(CELL_TWEETS_PATH, 'w') as stream:
        json.dump(
            dict(filter(lambda item: len(item[1]) > 0, cells.items())),
            stream
        )


def create_tweet_dict(coords, tweet):
    """
    Return a custom tweet dictionary for serialization

    Coords: Coordinates of tweet
    Tweet:  Dictionary of tweet metadata
    """

    # The http://dbpedia.org links are very regular, so we can
    # always take the last branch as what we want. No parsing
    # required.
    # We need to unquote the URL though to get escaped characters
    # http://dbpedia.org replaces spaces with '_', so then replace
    # the spaces back into the term for our purposes.
    features = [
        urllib.parse.unquote(feature.split('/')[-1]).replace('_', ' ') for feature in tweet['entities']
    ]
    return {
        'acheneID': tweet['municipality.acheneID'],
        'coords': coords,
        'lang': tweet['language'],
        'entities': tweet['entities'],
        'features': features,
        'user': tweet['user']
    }


def unique_users():
    """
    Determine the number of unique users for all tweets in a given census block.
    Calculate the average number across all blocks (with tweets only) as well.
    """

    blockTweets = {}
    with open(CENSUS_BLOCKS_TWEETS_PATH, 'r') as stream:
        blockTweets = json.load(stream)

    s = 0
    n = 0
    for block, tweets in blockTweets.items():

        users = set()
        for tweet in tweets:
            users.add(tweet['user'])

        print(block, len(users))
        n += 1
        s += len(users)

    print(s / n) # 12.147690069116042


if __name__ == "__main__":
    #get_milan_polygons()
    #get_cells_tweets_map()
    #prune_cell_tweets()
    #get_census_tweets_map()
    #unique_users()
