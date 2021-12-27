import subprocess

def execute_cmd(cmd):
    process = subprocess.Popen(tuple(cmd), stdout=subprocess.PIPE)
    for line in iter(process.stdout.readline, b''):
        print(line.decode("utf-8"), end='')