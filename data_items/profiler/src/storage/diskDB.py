# from elasticsearch import Elasticsearch, ElasticsearchException
# from elasticsearch.helpers import parallel_bulk
from logger import Logger
from storage.i_documentDB_disk import IDocumentDB_disk
from storage.utils import serialize_profiles#, serialize_rawData,
import json

logger = Logger(__name__).create_console_logger()



class DiskDB(IDocumentDB_disk):
    col_count = 0

    # def __init__(self, host: str = 'localhost', port: int = 9200):
    #     # self.f = open('data.json', 'a+', encoding='utf-8')
    #     # self.f.write('[')

    # def close_db(self):
    #     pass
    '''
    def store_data_disk(self, rawData: list):

        s, column = serialize_rawData(rawData)
        if not (s is None):
            f = open('meta_data/raw_data/{}.json'.format(column))
            json.dump(s, f, ensure_ascii=False, indent=4)
            f.close()
    '''
    def store_profiles_disk(self, profiles: list):
        profiles = serialize_profiles(profiles)
        for p in profiles:
            self.col_count = self.col_count + 1
            #col_name = p['_source']['columnName'].strip('/.,"` ')
            #col_id = p['_source']['id']
            f = open('storage/meta_data/profiles/{}_column.json'.format(self.col_count), 'w')
            json.dump(p, f, ensure_ascii=False, indent=4)
            f.close()

        '''
        s = serialize_profiles(profiles)
        if not (s is None):
            col_name = s['_source']['columnName']
            col_id = s['_source']['id']
            f = open('storage/meta_data/profiles/{}_{}.json'.format(col_name, col_id), 'w')
            #f = open('storage/meta_data/profiles/x.json', 'w')
            json.dump(s, f, ensure_ascii=False, indent=4)
            f.close()
        '''