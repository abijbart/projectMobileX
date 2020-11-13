#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written for Python 3.7.3

Create census block to census block graph of calls over Milan from cell-to-cell
graph

"Block" means census blocks.
"Cell" means the 0.25 km2 Milan cells that the cells that the cell call data was
prescribed to.

Since there was no framework design when Robert began manipulating the data,
parts of many of the functions found here can be found in other .pys in the
repository.

This file is separate from mi_to_mi.py just to keep things simpler.
No fundamental reason why this was necessary (it wasn't).
"""


from collections import defaultdict
import csv
import json
import os
from pathlib import Path
import pickle

import networkx
import pandas as pd



DATA_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
# Reverse json mapping, blocks-to-cell instead of cells-to-blocks
BLOCK_MAPPING_PATH = Path(
    DATA_DIR.parent,
    'Milan-Census-Mapping',
    'milan_grid_census_codes_map_percents_REVERSE.json'
)
# Census block to census block graph
BLOCK_GRAPH_PATH = Path(
    DATA_DIR,
    'milian_to_milian_census_blocks_weighted_undir_graph_aggregate_November.pickle'
)
# Census block to census block graph, with each node containing all census
# attributes
BLOCK_GRAPH_ATTR_PATH = Path(
    DATA_DIR,
    'milian_to_milian_census_blocks_weighted_undir_graph_aggregate_November_attributes.pickle'
)
# Census block to census block graph, with each node containing all census
# attributes, but represented as a CSV instead of a NetworkX graph
BLOCK_GRAPH_ATTR_CSV_PATH = Path(
    DATA_DIR,
    'milian_to_milian_census_blocks_weighted_undir_graph_aggregate_November_attributes.csv'
)
# Cell to cell Milan graph
CELL_GRAPH_PATH = Path(
    DATA_DIR,
    'milian_to_milian_cells_weighted_undir_graph_aggregate_November.pickle'
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
# JSON list of blocks in graphs that do not have any census data
MISSING_BLOCKS_PATH = Path(DATA_DIR, 'missing_blocks.json')


def get_census_blocks_graph():
    """
    Unpickle the census block to census block graph

    Graph includes census attributes on every node
    """

    with open(BLOCK_GRAPH_ATTR_PATH, 'rb') as stream:
        return pickle.load(stream)


def add_census_attributes():
    """
    Given the block-to-block graph, add attributes to every node representing
    census information

    Writes the graph out to disk
    """

    blocksAttr = {}
    blocksGraph = None

    with open(BLOCK_GRAPH_PATH, 'rb') as stream:
        blocksGraph = pickle.load(stream)

    with open(CENSUS_DATA_JSON_PATH, 'r') as stream:
        blocksAttr = json.load(stream)

    # Some blocks are missing census information. Record what they are
    missingBlocks = set()
    for node in blocksGraph.nodes():
        try:
            for key, attr in blocksAttr[str(node)].items():
                blocksGraph.nodes[node][key] = attr
        except KeyError:
            missingBlocks.add(str(node))

    with open(BLOCK_GRAPH_ATTR_PATH, 'wb') as stream:
        pickle.dump(blocksGraph, stream)

    with open(MISSING_BLOCKS_PATH, 'w') as stream:
        json.dump(list(missingBlocks), stream)


def create_blocks_dataframe():
    """
    Taking the block-to-block graph, create a CSV file representing the graph

    The CSV file is creating using Pandas in the hope that Pandas will read
    it in without issues

    The tuples are represented in the CSV under the column "EdgeTuples".
    Each EachTuple is a tuple of the form (Node, Weight)
    Nodes are identified as in the block-to-block graph, the SEZ2011 identifier
    """

    blocksGraph = get_census_blocks_graph()
    graphLists = []
    keysList = []

    for (node, attrs) in blocksGraph.nodes(data=True):

        if not keysList:
            keysList = list(attrs.keys())

        attrsList = list(attrs.values())
        attrsList.append([])
        for nodeFrom, nodeTo, edgeAttr in blocksGraph.edges(node, data=True):
            attrsList[-1].append((nodeTo, edgeAttr['weight']))

        graphLists.append(attrsList)

    keysList.append('EdgeTuples')

    dataFrame = pd.DataFrame(graphLists, columns=keysList)
    # Pandas might be able to handle a Path object
    # Also, maybe not
    # Not worth the risk since the above logic doesn't finish instantaneously,
    # so just convert to a string
    dataFrame.to_csv(str(BLOCK_GRAPH_ATTR_CSV_PATH))


def create_census_blocks_graph():
    """
    Create and write to disk a block-to-block graph using the cell-to-cell
    graph as a base and the block-to-cell (and cell-to-block) mapping(s) as
    a map
    """

    blocksGraph = networkx.Graph()
    blocksMap = {}
    cellsGraph = None
    cellsMap = {}

    with open(BLOCK_MAPPING_PATH, 'r') as stream:
        blocksMap = json.load(stream)
    with open(CELL_GRAPH_PATH, 'rb') as stream:
        cellsGraph = pickle.load(stream)
    with open(CELL_MAPPING_PATH, 'r') as stream:
        cellsMap = json.load(stream)

    for SEZ2011, cellsFrom in blocksMap.items():

        SEZ2011 = int(SEZ2011)
        if not blocksGraph.has_node(SEZ2011):
            blocksGraph.add_node(SEZ2011)

        for cellFrom in cellsFrom:
            for cellNodeFrom, cellNodeTo, cellEdgeAttributes in cellsGraph.edges(
                [int(cellFrom['cellID'])], data=True
            ):
                for blockTo in cellsMap[str(cellNodeTo)]:

                    destSEZ2011 = int(blockTo['censusCodes']['SEZ2011'])
                    if not blocksGraph.has_node(destSEZ2011):
                        blocksGraph.add_node(destSEZ2011)

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
                    # Since the graph is undirected, we're going to count twice
                    # using this method.
                    # Instead of trying to remember what cells we've recorded,
                    # just divide all intensities by 2 afterwards
                    weight = (
                        cellEdgeAttributes['weight']
                        * cellFrom['censusAreaPercentage']
                        * blockTo['censusAreaPercentage']
                    )
                    try:
                        blocksGraph.edges[SEZ2011, destSEZ2011]['weight'] += weight
                    except KeyError:
                        blocksGraph.add_edge(
                            SEZ2011, destSEZ2011, weight=weight
                        )

    # Normalize for double counting earlier
    for (node1, node2) in blocksGraph.edges():
        blocksGraph.edges[node1, node2]['weight'] /= 2

    with open(BLOCK_GRAPH_PATH, 'wb') as stream:
        pickle.dump(blocksGraph, stream)


def get_census_dict():
    """
    Load census information JSON

    If JSON does not exist, create it from the census CSVs found in the
    CENSUS_DATA_DIR. Write it out to disk before returning the information
    """

    if CENSUS_DATA_JSON_PATH.exists():
        with open(CENSUS_DATA_JSON_PATH, 'r') as stream:
            return json.load(stream)

    if not CENSUS_DATA_DIR.is_dir():
        raise FileNotFoundError(CENSUS_DATA_DIR)

    censusBlocks = {}
    for path in CENSUS_DATA_DIR.iterdir():

        # We're only interested in the CSVs with census information
        # They all start with "R", and nothing else in the provided directory
        # from the Italian census bureau do.
        if not path.name.startswith('R'):
            continue

        # Sanity print
        print(path)

        # NOTE The input CSVs are encoded using ISO-8859-1 for many of the
        # diacritics, etc. naturally found in location names
        # Robert forgot to encode the output JSON similarly
        # The output looks OK, but take that into consideration if using the
        # names directly (I doubt we will, which is why this wasn't fixed.)
        with open(path, 'r', encoding="ISO-8859-1") as stream:

            for block in csv.DictReader(stream, delimiter=';'):
                for key, val in block.items():
                    try:
                        block[key] = int(val)
                    except ValueError:
                        pass
                censusBlocks[block['SEZ2011']] = block

    with open(CENSUS_DATA_JSON_PATH, 'w') as stream:
        json.dump(censusBlocks, stream)

    return censusBlocks


def census_block_graph_fill_in_holes():
    """
    Given the block-to-block graph, fills in SEZ2011 attributes for those blocks
    that have no census information

    The SEZ2011 attribute is known since that's the key for the block

    Also some sanity checking of the node attributes
    """

    blocksGraph = get_census_blocks_graph()
    keys = []
    for (node, attrs) in blocksGraph.nodes(data=True):
        if attrs:
            keys = attrs.keys()
            break

    for (node, attrs) in blocksGraph.nodes(data=True):
        if not attrs:
            for key in keys:
                blocksGraph.nodes[node][key] = None
        if blocksGraph.nodes[node]['SEZ2011'] is None:
            blocksGraph.nodes[node]['SEZ2011'] = node

    # Didn't know the standard length, so just trust the first entry
    # They're all supposed to be the same.
    # A small sample is accurate, so we can trust that if all are the same
    # all are accurate
    length = -1
    for (node, attrs) in blocksGraph.nodes(data=True):
        if length < 0:
            length = len(attrs)
        else:
            if length != len(attrs):
                raise ValueError(f'{node} {length} {len(attrs)}')

    with open(BLOCK_GRAPH_ATTR_PATH, 'wb') as stream:
        pickle.dump(blocksGraph, stream)


def milan_grid_census_blocks_map_reverse():
    """
    Reverse the cell-to-block mapping to create (and write out) a block-to-cell
    mapping
    """

    if not DATA_DIR.is_dir():
        raise FileNotFoundError(DATA_DIR)

    blockMap = defaultdict(list)
    cellMap = {}
    with open(CELL_MAPPING_PATH, 'r') as stream:
        cellMap = json.load(stream)

    for cellId, blocks in cellMap.items():

        if len(blocks) == 0:
            continue

        for block in blocks:

            copy = {'cellID': cellId}
            # Shallow copy
            # No manipulations, so this is safe
            for key, val in block.items():
                copy[key] = val

            blockMap[block['censusCodes']['SEZ2011']].append(copy)

    with open(BLOCK_MAPPING_PATH, 'w') as stream:
        json.dump(blockMap, stream)


if __name__ == '__main__':
    #milan_grid_census_blocks_map_reverse()
    #get_census_dict()
    #create_census_blocks_graph()
    #add_census_attributes()
    #census_block_graph_fill_in_holes()
    create_blocks_dataframe()
