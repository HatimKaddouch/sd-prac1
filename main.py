import yaml
import orchestrator
import sys

def sfile(result, function):
    file = open(function+".json","w+")
    file = write(result)
    file.close()

def main():
    with open('ibm_cloud_config', 'r') as config_file:
        res = yaml.safe_load(config_file)
    key = sys.argv[1]
    maps = sys.argv[2]
    o = Orchestrator(key, maps, res)
    result1 = o.map_reduce('word_count')
    result2 = o.map_reduce('counting_words')
    sfile(result1, 'word_count')
    sfile(result2, 'counting_words')
    exit (0)

if __name__ == "__main__":
    main()
