# data_MaSyMos
MaSyMos Dataloader for Covidgraph

Loads a subset of the data of https://sems.bio.informatik.uni-rostock.de/projects/masymos/index.html into Covidgraph

Part of the Covidgraph pipeline: https://github.com/covidgraph/motherlode


MaSyMoS is short for "Management System for Models and Simulations". It contains simulation models in SBML and CellML format, simulation descriptions in SED-ML format and bio-ontologies.

## Related Blog Post
https://healthecco.org/healthecco/masymos/


# Run

Requirements:
* Running Neo4j DB


## Docker

todo!

## Local

Requirements:
* Python3 installed with pip

`pip install -r reqs.txt`

Provide source file (not provided atm until clarified if its non confidential)

`export SOURCE_FILE_URL=https://<URL-TO-SOURCEFILE>`

Set DB connection:

`export NEO4J='{"host":"localhost","port":7687,"user":"neo4j","password":"wurscht"}'`

Start script

`python3 ./dataloader/main.py`
