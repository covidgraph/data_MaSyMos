import zipfile
import os
import sys
import urllib.request
import py2neo
import json

if __name__ == "__main__":
    SCRIPT_DIR = os.path.dirname(
        os.path.realpath(os.path.join(
            os.getcwd(), os.path.expanduser(__file__)))
    )
    PARENT_DIR = os.path.join(SCRIPT_DIR, "..")
    sys.path.append(os.path.normpath(PARENT_DIR))


DATASOURCE_BASE_PATH = os.path.join(
    PARENT_DIR, "datasource")
DATASOURCE_PATH = os.path.join(DATASOURCE_BASE_PATH, "MaSyMoS_data")

NEO4J_CONFIG_STRING = os.getenv("NEO4J", "{}")
NEO4J_CONFIG_DICT = json.loads(NEO4J_CONFIG_STRING)
g = py2neo.Graph(**NEO4J_CONFIG_DICT)


def clean_data_sources():
    import shutil

    shutil.rmtree(DATASOURCE_BASE_PATH)
    os.makedirs(DATASOURCE_BASE_PATH)


def download_data():

    print('Beginning file download with urllib2...')

    url = 'https://nc.covidgraph.org/s/znfmiHTBkZgtW4x/download'
    target_file = os.path.join(
        DATASOURCE_BASE_PATH, "zipped.zip")
    target_unzipped_location = DATASOURCE_BASE_PATH
    urllib.request.urlretrieve(url, target_file)

    with zipfile.ZipFile(target_file, 'r') as zip_ref:
        zip_ref.extractall(target_unzipped_location)


def commit(statements):
    #tx = g.begin()
    all_statements_string = "".join(statements)
    all_statements_list = all_statements_string.split(";")
    for statement in all_statements_list:
        try:
            statement = statement.replace('\r', ' ').replace('\n', ' ').strip()
            if statement != "":
                g.run(statement)
        except Exception as e:
            if "AlreadyExists" in str(e):
                continue
            else:
                raise e

    # tx.commit()


def parse_cypher_file(filename):
    print(filename)
    with open(filename) as f:
        lines = f.readlines()
    cypher_statement = []
    record = True
    for line in lines:
        if line.replace('\r', '').replace('\n', '') == ":begin":
            record = True
            continue
        elif line.replace('\r', '').replace('\n', '') == ":commit":
            record = False
            commit(cypher_statement)
            cypher_statement = []
            continue
        if record:
            cypher_statement.append(line)


clean_data_sources()
download_data()


for filename in os.listdir(DATASOURCE_PATH):
    if filename.endswith(".cypher"):
        print(filename)
        parse_cypher_file(os.path.join(DATASOURCE_PATH, filename))
