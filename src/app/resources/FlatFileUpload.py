from flask import current_app
from flask_restful import Resource, reqparse, request
from werkzeug.utils import secure_filename

from services.flat_file.flat_file import FlatFile
from services.file_services.local_file_service import LocalFileService
from services.jsondb import JsonDb


LOCAL_FILE_DIRECTORY = '/Users/jamesramsay/Repos/flat-file-manager/src/user_files'

class FlatFileUploadResource(Resource):

  def get(self):
    db = JsonDb()
    return db.get_all()

  def post(self):

    fs = LocalFileService(LOCAL_FILE_DIRECTORY)
    local_file_path = fs.get_csv_file_path()
    user_file = request.files['file']
    clean_filename = secure_filename(user_file.filename)
    user_file.save(local_file_path)

    ff = FlatFile(local_file_path, original_file_name=clean_filename)
    descriptor = ff.get_file_descriptor()

    db = JsonDb()
    db.set_by_key(descriptor.unique_id, descriptor)

    return {
      'local_file_path': local_file_path,
      'clean_filename': clean_filename
    }
