
from common.utils.json_encoder import EnhancedJSONEncoder

class Config(object):
    RESTFUL_JSON = {
        'separators': (', ', ': '),
        'indent': 2,
        'cls': EnhancedJSONEncoder
    }
