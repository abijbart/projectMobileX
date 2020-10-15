#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Written for Python 3.7.3

Program to create a relation graph for Milan to Milan data
"""


from collections import defaultdict
import gc
import json
import os
from pathlib import Path
import pickle

import networkx


# These paths must be changed to wherever the data is
DATA_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
CELL_MAPPING_PATH = Path(DATA_DIR.parent, 'MilanCensusMapping', 'milan_grid_census_codes_map.json')


def relation_graph_to_json():
    """
    For each Milan to Milan tab separate file, create an undirected graph
    between each Milan grid cell with a connection, counting the number
    of connections between each cell.

    The graph is represented as a dictionary of dictionaries:
    {nodeID: {nodeID: count, ...}}

    The dictionary is then written out as a JSON file.

    The Milan to Milan files are huge (~6GB per day) and so are not a part
    of the repository
    They can be found here:
    https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/JZMTBJ
    So this function doesn't work as constituted in the repository.
    """

    if not DATA_DIR.is_dir():
        raise FileNotFoundError(DATA_DIR)

    # Index var instead of enumerate() because there is likely non-.txt files
    # within the directory
    index = 0
    for filePath in DATA_DIR.iterdir():

        if filePath.suffix != '.txt':
            continue

        index += 1
        with open(filePath, 'r') as stream:

            # Sanity check print
            print(filePath)

            graph = defaultdict(lambda: defaultdict(float))
            for line in stream:

                # Files are tab separated
                # tokens[0] is the beginning (unix epoch) of the 10 minute time
                #   duration
                # tokens[1] is the from cell
                # tokens[2] is the to cell
                # tokens[3] is a value representing the directional interaction
                #   strength between Square id1 and Square id2. It is
                #   proportional to the number of calls exchanged between
                #   callers, which are located in Square id1, and receivers
                #   located in Square id2;
                tokens = line.split('\t')
                fromT = int(tokens[1])
                toT = int(tokens[2])
                if fromT > toT:
                    fromT, toT = toT, fromT
                graph[fromT][toT] += float(tokens[3])

        graphDict = {}
        for key, val in graph.items():
            graphDict[key] = dict(val)

        with open(f'milian_to_milian_weighted_undir_graph_11_{index}.json', 'w') as stream:
            json.dump(graphDict, stream)

def aggregate_dates():
    """
    Aggregate individual day .json maps into one NetworkX graph.
    NetworkX graph only contains nodes that are covered by census blocks.
    Otherwise the graph is too large to fit in 8GB of memory
    """

    if not DATA_DIR.is_dir():
        raise FileNotFoundError(DATA_DIR)

    graph = networkx.Graph()
    grid = None
    # Those cellIDs that are coverd (at least partially) by a census block
    milanNodes = set()
    with open(CELL_MAPPING_PATH, 'r') as stream:
        grid = json.load(stream)

    for cellId, censusBlocks in grid.items():
        if len(censusBlocks) > 1:
            milanNodes.add(int(cellId))

    for node in milanNodes:
        graph.add_node(node)

    for filePath in DATA_DIR.iterdir():

        if filePath.suffix != '.json':
            continue

        # Sanity print
        print(filePath)

        with open(filePath, 'r') as stream:
            dictionary = json.load(stream)

        # This will clear the old dictionary object, which is about 4GB
        # Python is lazy about it. I ran out of memory without the hint
        gc.collect()

        for node1, innerDict in dictionary.items():

            node1 = int(node1)
            if node1 not in milanNodes:
                continue

            for node2, w in innerDict.items():

                node2 = int(node2)
                if node2 not in milanNodes:
                    continue

                w = float(w)

                # If the edge exists in the graph, add to the weight
                # instead of replacing the existing weighted edge.
                try:
                    graph.edges[node1, node2]['weight'] += w
                except KeyError:
                    graph.add_edge(node1, node2, weight=w)

        with open(f'milian_to_milian_weighted_undir_graph_aggregate.pickle', 'wb') as stream:
            pickle.dump(graph, stream)


if __name__ == "__main__":
    #relation_graph_to_json()
    #aggregate_dates()
