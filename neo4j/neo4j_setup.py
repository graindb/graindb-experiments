import os

import config
import utils

neo4j_url = 'https://neo4j.com/' + config.neo4j_tar

# download neo4j community version
if __name__ == '__main__':
    if not os.path.exists(config.neo4j_dir):
        os.makedirs(config.neo4j_dir)
    os.chdir(config.neo4j_dir)
    
    if not os.path.exists(config.neo4j_tar):
        download_cmd = ['wget', neo4j_url]
        utils.execute_cmd(download_cmd)
    
    unzip_cmd = ['tar', '-zxvf', config.neo4j_tar]
    utils.execute_cmd(unzip_cmd)

