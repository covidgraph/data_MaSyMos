from typing import List
import zipfile
import os
import sys
import urllib.request
import py2neo
import json
from linetimer import CodeTimer

if __name__ == "__main__":
    SCRIPT_DIR = os.path.dirname(
        os.path.realpath(os.path.join(
            os.getcwd(), os.path.expanduser(__file__)))
    )
    PARENT_DIR = os.path.join(SCRIPT_DIR, "..")
    sys.path.append(os.path.normpath(PARENT_DIR))


# Source data. A zip file containing one or more '*.cypher'-file of cypher statements
SOURCE_FILE_URL = os.getenv("SOURCE", None)
if not SOURCE_FILE_URL:
    raise ValueError(
        "No source file url provided. Please set the environment variable 'SOURCE'")
# generate target path for source data dir
DATASOURCE_BASE_PATH = os.path.join(
    PARENT_DIR, "datasource")
# generate target path for source data zip file
DATASOURCE_PATH = os.path.join(DATASOURCE_BASE_PATH, "MaSyMoS_data")

# Read environemnt variable NEO4J, which contains database connection information as json.
# keys and values matching the parameters in https://py2neo.org/v4/database.html#the-graph
NEO4J_CONFIG_STRING = os.getenv("NEO4J", "{}")
# Convert json string into python dict
NEO4J_CONFIG_DICT = json.loads(NEO4J_CONFIG_STRING)
# Create database connection
g = py2neo.Graph(**NEO4J_CONFIG_DICT)


def clean_data_sources():
    import shutil

    shutil.rmtree(DATASOURCE_BASE_PATH)
    os.makedirs(DATASOURCE_BASE_PATH)


def download_data():

    url = SOURCE_FILE_URL
    target_file = os.path.join(
        DATASOURCE_BASE_PATH, "zipped.zip")
    print(f"Beginning file download file '{url}' to '{target_file}'...")
    target_unzipped_location = DATASOURCE_BASE_PATH
    urllib.request.urlretrieve(url, target_file)
    print(
        f"Extracting file '{target_file}' to directory '{target_unzipped_location}'...")
    with zipfile.ZipFile(target_file, 'r') as zip_ref:
        zip_ref.extractall(target_unzipped_location)


def commit(transaction_block: List[str]):
    """[summary]

    Args:
        transaction_block (List[str]): List of cypher statements

    Raises:
        e: [description]
    """
    # ToDo: Instead of running statement by statement we could use https://neo4j.com/labs/apoc/4.1/cypher-execution/cypher-multiple-statements/
    # evaluate benefit: Needs apoc but could be more perfomant

    print(
        f"Run next transaction block of {len(transaction_block)} statements...")
    tx = g.begin()
    for index, statement in enumerate(transaction_block):
        try:
            # remove any trailing
            statement = statement.replace(
                '\r', ' ').replace('\n', ' ').strip()
            if statement != "":
                tx.run(statement)
        except Exception as e:
            if "AlreadyExists" in str(e) and len(transaction_block) == 1:
                print(
                    f"Statement:\n'{statement}'\ncaused an 'Already Exists' error. Will ignore and continue... ")
                return
            else:
                print(
                    f"Failed statement: '{statement}'\nStatement {index+1} of {len(transaction_block)} in statement block")
                raise e
    print(f"..Ran {len(transaction_block)} statements. Now commiting...")
    tx.commit()
    print(f"..Block is finished.")


def isolate_single_statements_in_transaction_block(block: List[str]):
    """Merges lines that coherent cypher statements and splits multiple cypher statements

    Args:
        block (List[str]): List of strings which are fragments of multiple cypher statements

    Returns:
        List[str]: List of single cypher statements
    """
    statements = []
    current_statement = ""
    for line in block:
        current_statement = f"{current_statement} {line}"
        if line.endswith(";"):
            statements.append(current_statement)
            current_statement = ""
    return statements


def parse_cypher_file(cypher_file_path: str):
    """Reads a file of cypher statements line by line and splits transaction blocks. A block has to be seperated by with ":begin" and ":commit"

    Args:
        cypher_file_path (str): name of the file to be parsed
    """
    print("Run Queries")

    # ToDo: To be compatible with very large files, we could stream the lines here, instead of loading everything in memory at once
    with open(cypher_file_path) as f:
        lines = f.readlines()
    cypher_transaction_block = []
    record = True
    for line in lines:
        stripped_line = line.replace('\r', '').replace('\n', '').strip()
        if stripped_line == ":begin":
            record = True
            continue
        elif stripped_line == ":commit":
            record = False
            commit(
                isolate_single_statements_in_transaction_block(cypher_transaction_block))
            cypher_transaction_block = []
            continue
        if record:
            # ignore empty line. only record lines with content
            if stripped_line:
                cypher_transaction_block.append(stripped_line)


# Delete existing/old source files, to prevent confusion with newer files
clean_data_sources()
# Download source files
download_data()


for filename in os.listdir(DATASOURCE_PATH):
    if filename.endswith(".cypher"):
        print(f"Start processing '{filename}'")
        with CodeTimer(filename, unit="s"):
            parse_cypher_file(os.path.join(DATASOURCE_PATH, filename))
