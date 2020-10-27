#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written for Python 3.7.3

Program to create a relation map between Milan cell IDs and the census location
codes necessary to look up census data for the location in question
"""


import json
import os
from pathlib import Path
import re

# Requires geojson. Can be pip installed
import geojson
# Requires shapely. Can be pip installed
from shapely.geometry import Polygon


# You will need to change these paths to match your local system
DATA_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
MILAN_GRID_GEOJSON = Path(DATA_DIR.parent, 'dataset/milano-grid.geojson')
# The census codes are burried in HTML in the geojson
# Parsing the HTML is excessive. The HTML is small enough that it's easier to
# grab the codes like this.
GEOJSON_CODES_REGEX = re.compile(r'<td>(?P<code>[A-za-z][^<]*)<\/td>.*?<td>(?P<val>\d+)', re.S)


def create_sezioni_censimento_millan_grid_map():

    # Mapping grid to write out file
    # This dictionary keys are Milan grid cell IDs
    # Each value is a list of the census location dictionaries that overlap
    # with the cell
    # Each census location dictionary has a series of census codes that
    # can be used to lookup the census block
    grid = {}
    # List of census polygons and descriptive information
    censusPolygons = []
    # List of Milan cell polygons and descriptive information
    milanPolygons = []

    censusGeojsonPath = Path(DATA_DIR, 'CensusGeojson')
    for geojsonPath in censusGeojsonPath.iterdir():
        with open(geojsonPath, 'r') as stream:

            geojsonDump = geojson.load(stream)
            for feature in geojsonDump['features']:

                if (
                    feature['type'] == 'Feature'
                    and feature['geometry']['type'] == 'Polygon'
                ):

                    matches = GEOJSON_CODES_REGEX.finditer(
                        feature['properties']['description']
                    )
                    if matches:

                        # For some reason there's an extra list on the cooridnates
                        # That messes up the contructor of Polygon
                        censusPolygon = {
                            'polygon': Polygon(feature['geometry']['coordinates'][0])
                        }
                        for match in matches:
                            censusPolygon[match.group('code').replace('_', '')] = match.group('val')

                        censusPolygons.append(censusPolygon)

    with open(MILAN_GRID_GEOJSON, 'r') as stream:
        geojsonDump = geojson.load(stream)

        for feature in geojsonDump['features']:

            if feature['geometry']['type'] == 'Polygon':

                # For some reason there's an extra list on the cooridnates
                # That messes up the contructor of Polygon
                milanPolygon = {
                    'cellId': feature['properties']['cellId'],
                    'polygon': Polygon(feature['geometry']['coordinates'][0])
                }
                milanPolygons.append(milanPolygon)

    # If the polygons overlap, add the census information to the cell's
    # list of census blocks
    for milanPolygon in milanPolygons:

        milanArea = milanPolygon['polygon'].area
        grid[milanPolygon['cellId']] = []
        for censusPolygon in censusPolygons:

            censusArea = censusPolygon['polygon'].area
            if milanPolygon['polygon'].overlaps(censusPolygon['polygon']):

                # Save the percentage area overlap in 'areaPercentage'
                # and the codes in 'censusCodes'
                # Creating a list of dictionaries with these two keys
                intersectArea = milanPolygon['polygon'].intersection(
                    censusPolygon['polygon']
                ).area
                milanAreaPercentage = intersectArea / milanArea
                censusAreaPercentage = intersectArea / censusArea

                # Round what is effectively float error
                if milanAreaPercentage < 1e-4:
                    milanAreaPercentage = 0
                elif milanAreaPercentage > 0.9999:
                    milanAreaPercentage = 1
                if censusAreaPercentage < 1e-4:
                    censusAreaPercentage = 0
                elif censusAreaPercentage > 0.9999:
                    censusAreaPercentage = 1

                milanSection = {
                    'censusAreaPercentage': censusAreaPercentage,
                    'milanAreaPercentage': milanAreaPercentage,
                    'censusCodes': censusPolygon
                }
                grid[milanPolygon['cellId']].append(milanSection)

    # Don't write the Polygons out to file
    # The polygons are a shared structure, so delete them now instead of
    # earlier
    for val in grid.values():
        for censusPolygon in val:
            try:
                del censusPolygon['censusCodes']['polygon']
            except KeyError:
                pass

    with open('milan_grid_census_codes_map_percents_both.json', 'w') as stream:
        json.dump(grid, stream)


if __name__ == "__main__":
    create_sezioni_censimento_millan_grid_map()
