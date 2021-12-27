import os
import glob
import shutil

import config
import utils

LDBC_LOAD_CMD = [
    "./bin/neo4j-admin", 
    "import", 
    "--database=neo4j", 
    "--delimiter=,",
    "--id-type=INTEGER",
    "--nodes=Person=import/v-person.csv",
    "--relationships=knows=import/e-knows.csv",
]

if __name__ == '__main__':
    if not os.path.exists(config.neo4j_community):
        print(config.neo4j_community + " does not exist. Call setup.py first.")

    for file in glob.glob(config.dataset_path + '/*.csv'):
        shutil.copy(file, config.neo4j_community + '/import')

    os.chdir(config.neo4j_community)
    utils.execute_cmd(LDBC_LOAD_CMD)

    
