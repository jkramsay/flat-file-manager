from flask_restful import Resource
from services.jsondb import JsonDb

class FlatFileResource(Resource):

  def get(self, file_id=None):
    db = JsonDb()
    try:
      file_descriptor = db.get_by_key(file_id)
      return file_descriptor
    except:
      return {
        'error': 'FILE_NOT_FOUND',
        'message': 'File {0} not found'.format(file_id)
      }, 404
