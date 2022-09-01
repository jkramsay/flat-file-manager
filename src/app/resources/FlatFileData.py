from flask_restful import Resource

from services.jsondb import JsonDb
from services.flat_file.flat_file import FlatFile

class FlatFileDataResource(Resource):
   
  def get(self, file_id):
    db = JsonDb()
    try:
      file_descriptor = db.get_by_key(file_id)
      ff = FlatFile(file_descriptor['local_file_path'])
      return ff.get_records()
    except Exception as e:
        return {
            'error': 'FILE_NOT_FOUND',
            'message': 'File {0} not found'.format(file_id)
        }, 404
