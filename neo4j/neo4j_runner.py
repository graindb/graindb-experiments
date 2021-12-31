from neo4j import GraphDatabase, unit_of_work
import time
import sys

import config

@unit_of_work(timeout=300)
def query_fun(tx, query):
    result = tx.run(query)
    return result.single()

def run_query(session, query_name, query, results_file):
    # one wram up
    result = session.read_transaction(query_fun, query)
    # actual run
    start = time.time()
    result = session.read_transaction(query_fun, query)
    end = time.time()
    duration = end - start
    print("-----------------")
    print("Query: " + str(query_name))
    print("Duration: " + str(duration))
    print("Result: " + str(result[0]))
    results_file.write(f"{duration:.4f}\n")
    results_file.flush()

driver = GraphDatabase.driver(config.neo4j_url, auth=(config.neo4j_user, config.neo4j_pass))
session = driver.session()

# run micro_p
micro_p_cids = [82234254,82234384,82236234,82238034,82243534,82252934,82271834,82308434,82345034,82381534,82418234]
micro_p_idx = 1

with open(f"micro_p_neo_results.csv", "a+") as results_file:
    for cid in micro_p_cids:
        micro_p_query = "MATCH (p:Person)-[e:knows]->(:Person) WHERE p.cid<=" + str(cid) + " AND e.date>=0 RETURN COUNT(*);"
        micro_p_query_name = "Micro-P " + str(micro_p_idx)
        run_query(session, micro_p_query_name, micro_p_query, results_file)
        micro_p_idx = micro_p_idx + 1

# run micro_k
micro_k_dates = [1263736391, 1265366391, 1270216391, 1273536391, 1280516391, 1288866391, 1301606391, 1320486391, 1335216391, 1347496391, 1400000000]
micro_k_idx = 1

with open(f"micro_k_neo_results.csv", "a+") as results_file:
    for date in micro_k_dates:
        micro_k_query = "MATCH (p:Person)-[e:knows]->(:Person) WHERE p.cid>=0 AND e.date<=" + str(date) + " RETURN COUNT(*);"
        micro_k_query_name = "Micro-K " + str(micro_k_idx)
        run_query(session, micro_k_query_name, micro_k_query, results_file)
        micro_k_idx = micro_k_idx + 1


session.close()
driver.close()
