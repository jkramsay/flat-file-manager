import os
import json

from common.utils.json_encoder import EnhancedJSONEncoder

LOCAL_FILE_DIRECTORY = '/Users/jamesramsay/Repos/flat-file-manager/src/jsondb.json'

class JsonDb:
    def __init__(self, file_path=None):
        self.db = None
        self.file_path = LOCAL_FILE_DIRECTORY if file_path is None else file_path

    def get_all(self):
        result = []
        f = self._get_file()
        for key, val in f.items():
            result.append(val)
        return result
        
    def get_by_key(self, key):
        f = self._get_file()
        if key in f:
            return f[key]
        raise AssertionError('Key {0} not in file'.format(key))

    def set_by_key(self, key, val):
        f = self._get_file()
        f[key] = val
        self._write_file(f)

    def _get_file(self):
        if self.db is None:
            if os.path.isfile(self.file_path) == False:
                with open(self.file_path, "w") as outfile:
                    json.dump({}, outfile, cls=EnhancedJSONEncoder)
            
            with open(self.file_path) as json_file:
                    data = json.load(json_file)
                    self.db = data

        return self.db

    def _write_file(self, file_contents):
        with open(self.file_path, "w") as outfile:
            json.dump(file_contents, outfile, cls=EnhancedJSONEncoder)    
