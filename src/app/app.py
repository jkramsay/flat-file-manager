import sys
from pathlib import Path
path_dir = Path(__file__).parents[1]
sys.path.append(str(path_dir))

from flask import Flask
from flask_restful import Api
from flask_cors import CORS

from resources.FlatFile import FlatFileResource
from resources.FlatFileData import FlatFileDataResource
from resources.FlatFileUpload import FlatFileUploadResource

app = Flask(__name__)
app.config.from_object('config.Config')

cors = CORS(app, resources={r"/*": {"origins": "*"}})
api = Api(app)


# Flat File Resources
api.add_resource(FlatFileUploadResource, '/flatfile')
api.add_resource(FlatFileResource, '/flatfile/<string:file_id>')
api.add_resource(FlatFileDataResource, '/flatfile/<string:file_id>/data')


if __name__ == '__main__':
    app.run(host='localhost', port=5000, debug=True)


