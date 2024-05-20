from sqlalchemy import create_engine
import pymysql
import yaml

class Connection:
    def __init__(self, name: str = 'fsp_staging'):
        self.yaml_config = yaml.safe_load(open("utils/config.yaml"))
        assert name in self.yaml_config['db'].keys(), f'db name: {name} not in yaml file'
        self.yaml_db = name
        self.host = self.yaml_config['db'][self.yaml_db]['host']
        self.port = self.yaml_config['db'][self.yaml_db]['port']
        self.db = self.yaml_config['db'][self.yaml_db]['database']
        self.user = self.yaml_config['db'][self.yaml_db]['user']
        self.password = self.yaml_config['db'][self.yaml_db]['password']
        self.connect_str = self.yaml_config['db'][self.yaml_db]['connect_str']
        self._init = False
        
    def get_engine(self):
        if self.connect_str == 'pymysql':
            engine = pymysql.connect(host=self.host, user=self.user, password=self.password, db=self.db, port=self.port)
        else:
            engine = create_engine(f"{self.connect_str}://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}")
        return engine
        
    @property
    def init_(self):
        config = {
            'host':self.host,
            'port':self.port,
            'db':self.db,
            'user':self.user,
            'password':self.password
        }
        return config
        
    @property
    def dbnames_(self):
        return [f'name: {key} host: {self.yaml_config["db"][key]["host"]}' for key in self.yaml_config['db'].keys()]