import yaml
from orchestrator import *
import sys

def sfile(result, function):
    file = open(function+".json","w+")
    file.write(str(result))
    file.close()

def main():
    try:
        with open('ibm_cloud_config', 'r') as config_file:
            res = yaml.safe_load(config_file)
    except:
        print("ERROR: Fichero de configuracion.")
        exit(1)
    if len(sys.argv) != 3:
        print("ERROR: Parametros insuficientes.")
        exit(2)
    key = sys.argv[1]
    try:
        maps = int(sys.argv[2])
    except:
        print("ERROR: Maps tiene que ser entero")
        exit(3)
    o = Orchestrator(key, maps, res)
    result1 = o.map_reduce('word_count')
    result2 = o.map_reduce('counting_words')
    sfile(result1, 'word_count')
    sfile(result2, 'counting_words')
    exit (0)

if __name__ == "__main__":
    main()
