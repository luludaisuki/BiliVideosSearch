import yaml
def loadConfig(path):
    with open(path,'r')as f:
        config=yaml.load(f,Loader=yaml.FullLoader)

    return config