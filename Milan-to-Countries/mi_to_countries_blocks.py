"""
Written for Python 3.7.3

Create census block to census block graphs based on cell-to-country graphs.

"Block" means census blocks.
"Cell" means the 0.25 km2 Milan cells that the cells that the cell call data was
prescribed to.

Since there was no framework design when Robert began manipulating the data,
parts of many of the functions found here can be found in other .pys in the
repository.

This file is separate from mi_to_countries_blocks.py just to keep things simpler.
No fundamental reason why this was necessary (it wasn't).
"""


import json
import os
from pathlib import Path
import pickle

import networkx
import pandas as pd


DATA_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
CALL_BLOCK_GRAPH_PATH = Path(DATA_DIR, 'call-mi_blocks.pickle')
CALL_CELL_GRAPH_PATH = Path(DATA_DIR, 'call-mi.pickle')
INTERNET_BLOCK_GRAPH_PATH = Path(DATA_DIR, 'internet-mi_blocks.pickle')
INTERNET_CELL_GRAPH_PATH = Path(DATA_DIR, 'internet-mi.pickle')
SMS_BLOCK_GRAPH_PATH = Path(DATA_DIR, 'sms-mi_blocks.pickle')
SMS_CELL_GRAPH_PATH = Path(DATA_DIR, 'sms-mi.pickle')

# Reverse json mapping, blocks-to-cell instead of cells-to-blocks
BLOCK_MAPPING_PATH = Path(
    DATA_DIR.parent,
    'Milan-Census-Mapping',
    'milan_grid_census_codes_map_percents_REVERSE.json'
)
# Mapping of cells to census blocks, including the percetage of cell in census
# block, and vice-versa
CELL_MAPPING_PATH = Path(
    DATA_DIR.parent,
    'Milan-Census-Mapping',
    'milan_grid_census_codes_map_percents.json'
)
# Directory containing census information
CENSUS_DATA_DIR = Path(DATA_DIR.parent, 'dataset', 'Sezioni di Censimento')
# JSON of census data
CENSUS_DATA_JSON_PATH = Path(
    DATA_DIR.parent,
    'Milan-Census-Mapping',
    'Sezioni_di_Censimento.json'
)
COUNTRY_CODES_MAP_PATH = Path(DATA_DIR, 'country_codes_map.json')


def get_call_cell_graph():
    with open(CALL_CELL_GRAPH_PATH, 'rb') as stream:
        return pickle.load(stream)

def get_internet_cell_graph():
    with open(INTERNET_CELL_GRAPH_PATH, 'rb') as stream:
        return pickle.load(stream)

def get_sms_cell_graph():
    with open(SMS_CELL_GRAPH_PATH, 'rb') as stream:
        return pickle.load(stream)

def get_census_attributes():
    with open(CENSUS_DATA_JSON_PATH, 'r') as stream:
        return json.load(stream)

def get_cell_to_block_map():
    with open(CELL_MAPPING_PATH, 'r') as stream:
        return json.load(stream)

def get_block_to_cell_map():
    with open(BLOCK_MAPPING_PATH, 'r') as stream:
        return json.load(stream)

def get_country_codes_map():
    with open(COUNTRY_CODES_MAP_PATH, 'r') as stream:
        return json.load(stream)

def get_block_graph(blockPath, cellPath=None):
    """
    This function is akin to create_census_blocks_graph() from
    mi_to_mi_blocks.py, but applied to the datasets associated with
    Milan-to-Countries

    Takes a base cell-to-countries graph and creates a blocks-to-countries
    graph based off of the blocks-to-cells mapping JSON
    """

    if blockPath.exists():
        with open(blockPath, 'rb') as stream:
            return pickle.load(stream)

    if not cellPath:
        raise ValueError(
            "In order to create census blocks to countries graph, a base cell "
            + "to countries graph file path is necessary"
        )
    if not cellPath.exists():
        raise FileNotFoundError(cellPath)

    blocksGraph = networkx.Graph()
    cellsGraph = None
    blocksCellsMap = get_block_to_cell_map()

    with open(cellPath, 'rb') as stream:
        cellsGraph = pickle.load(stream)

    for SEZ2011, cellsFrom in blocksCellsMap.items():

        SEZ2011 = int(SEZ2011)
        if not blocksGraph.has_node(SEZ2011):
            blocksGraph.add_node(SEZ2011)

        for cellFrom in cellsFrom:

            # In cells graph, Milan cells are prefixed with 'm'
            for cellNodeFrom, cellNodeTo, cellEdgeAttributes in cellsGraph.edges(
                ['m' + cellFrom['cellID']], data=True
            ):

                # Sanity check that all from-nodes are indeed Milan nodes and
                # all to-nodes are country nodes
                if not str(cellNodeFrom).startswith('m'):
                    raise ValueError(f'From Node: {str(cellNodeFrom)}')
                if not str(cellNodeTo).startswith('c'):
                    raise ValueError(f'To Node: {str(cellNodeTo)}')

                if not blocksGraph.has_node(str(cellNodeTo)):
                    blocksGraph.add_node(str(cellNodeTo))

                # This is where the uniform distribution of cells and census
                # blocks comes into play.
                # We assume that the overlapping region of the cell and the
                # census block perfectly mimics the distribution of call
                # intensity over the cells, and census descriptives over the
                # census blocks.
                # Under this assumption, the weight is a weighted summation
                # of all the cell-to-cell edges composing the census blocks
                # based on the percentage of census blocks overlapping this
                # edge.
                #
                # Since there are no Milan-to-Milan edges, we do not double
                # count.
                #
                # Since the destination is countries and not blocks, we don't
                # need to adjust for an overlap area in the destination like
                # was true for the Milan-to-Milan call data.
                # Same reason we don't look through cellToBlock mappings.
                weight = (
                    cellEdgeAttributes['weight']
                    * cellFrom['censusAreaPercentage']
                )

                try:
                    blocksGraph.edges[SEZ2011, cellNodeTo]['weight'] += weight
                except KeyError:
                    blocksGraph.add_edge(
                        SEZ2011, cellNodeTo, weight=weight
                    )

    # Add census information to each node
    # For country nodes we don't have census information, but we can add
    # country name to the node (where we have it, we often don't)
    censusAttributes = get_census_attributes()
    countryCodesMap = get_country_codes_map()
    keys = []

    for node in blocksGraph.nodes:

        if str(node).startswith('c'):

            countryCode = str(node)[1:]
            countryName = countryCodesMap[countryCode]
            blocksGraph.nodes[node]['CountryCode'] = countryCode
            # Don't check for none. We want to save those Nones
            blocksGraph.nodes[node]['CountryName'] = countryName

        else:

            # Not all census blocks have census data
            # They've already been recorded by prior work
            try:
                if not keys:
                    keys = list(censusAttributes[str(node)].keys())
                for key, attr in censusAttributes[str(node)].items():
                    blocksGraph.nodes[node][key] = attr
            except KeyError:
                pass

    # We need to fill in Nones for those block nodes which don't have data
    for (node, attrs) in blocksGraph.nodes(data=True):
        if not str(node).startswith('c'):
            if not attrs:
                for key in keys:
                    blocksGraph.nodes[node][key] = None
            if blocksGraph.nodes[node]['SEZ2011'] is None:
                blocksGraph.nodes[node]['SEZ2011'] = node

    # Write the gtraph out
    with open(blockPath, 'wb') as stream:
        pickle.dump(blocksGraph, stream)

    return blocksGraph


def graph_to_csv(graphPath):
    """
    Since the countries nodes and census block nodes are split into two CSVs,
    the function is called twice
    """
    _graph_to_csv(graphPath, True)
    _graph_to_csv(graphPath, False)


def _graph_to_csv(graphPath, countries=False):
    """
    Taking a block-to-country graph, create a CSV file representing the graph

    The CSV file is creating using Pandas in the hope that Pandas will read
    it in without issues

    It seemed awkward to have both country nodes and census block nodes in the
    same CSV since their attributes as so wildly different, so they will be in
    two separate CSVs. Only one set of nodes is output in a given pass.
    """

    csvPath = Path()
    graph = None
    graphLists = []
    keys = []
    if countries:
        csvPath = Path(graphPath.parent, graphPath.stem + '_countires.csv')
    else:
        csvPath = Path(graphPath.parent, graphPath.stem + '_blocks.csv')

    with open(graphPath, 'rb') as stream:
        graph = pickle.load(stream)

    for (node, attrs) in graph.nodes(data=True):

        # This is lazy, but works
        if (
            (countries and not str(node).startswith('c'))
            or (not countries and str(node).startswith('c'))
        ):
            continue

        if len(keys) < len(attrs.keys()):
            keys = list(attrs.keys())

        attrsList = list(attrs.values())
        attrsList.append([])
        for _, nodeTo, edgeAttr in graph.edges(node, data=True):
            attrsList[-1].append((nodeTo, edgeAttr['weight']))

        graphLists.append(attrsList)

    keys.append('EdgeTuples')

    dataFrame = pd.DataFrame(graphLists, columns=keys)
    dataFrame.to_csv(str(csvPath))


if __name__ == '__main__':
    #call = get_block_graph(CALL_BLOCK_GRAPH_PATH, CALL_CELL_GRAPH_PATH)
    #internet = get_block_graph(INTERNET_BLOCK_GRAPH_PATH, INTERNET_CELL_GRAPH_PATH)
    #sms = get_block_graph(SMS_BLOCK_GRAPH_PATH, SMS_CELL_GRAPH_PATH)
    #graph_to_csv(CALL_BLOCK_GRAPH_PATH)
    #graph_to_csv(INTERNET_BLOCK_GRAPH_PATH)
    #graph_to_csv(SMS_BLOCK_GRAPH_PATH)
