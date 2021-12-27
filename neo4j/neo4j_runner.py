from neo4j import GraphDatabase, unit_of_work
import time
import sys

import config

@unit_of_work(timeout=300)
def query_fun(tx, query):
    result = tx.run(query)
    return result.single()

def run_query(session, query_spec, results_file):
    start = time.time()
    result = session.read_transaction(query_fun, query_spec)
    end = time.time()
    duration = end - start
    print("Duration: " + str(duration))
    print("Result: " + str(result[0]))
    print("-----------------")
    results_file.write(f"{duration:.4f}\n")
    results_file.flush()
    return (duration, result)

cids = [82234254,82234384,82236234,82238034,82243534,82252934,82271834,82308434,82345034,82381534,82418234]
index = 1

driver = GraphDatabase.driver(config.neo4j_url, auth=(config.neo4j_user, config.neo4j_pass))
session = driver.session()

with open(f"results.csv", "a+") as results_file:
    for cid in cids:
        micro_query = "MATCH (p:Person)-[e:knows]->(:Person) WHERE p.cid<=" + str(cid) + " AND e.date>=0 RETURN COUNT(*);"
        # warm up
        session.read_transaction(query_fun, micro_query)
        print("-----------------")
        print("Micro Query " + str(index))
        run_query(session, micro_query, results_file)
        index = index + 1

session.close()
driver.close()
