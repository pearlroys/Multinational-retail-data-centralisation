import yaml
from yaml.loader import SafeLoader
class DataConnector:
    def __init__(self) -> None:
        pass
    def read_db_creds(self):
        """ This function parse and converts a YAML object to a Python dictionary (dict object). 
        This process is known as Deserializing YAML into a Python."""
        # Open the file and load the file
        with open('db_creds.yaml') as f:
            data = yaml.safe_load(f)
            print(data)
        

if __name__ == '__main__':
    reader = DataConnector()
    reader.read_db_creds()