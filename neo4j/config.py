import os

root_dir = os.getcwd()
# dataset registrition
dataset_path = root_dir + "/dataset/"

# neo4j 
neo4j_tar = 'artifact.php?name=neo4j-community-4.3.1-unix.tar.gz'
neo4j_dir = root_dir + '/neo4j'
neo4j_community = neo4j_dir + '/neo4j-community-4.3.1'

# neo4j server connection
neo4j_url = 'neo4j://localhost:7687'
neo4j_user = 'neo4j'
neo4j_pass = '123456'

