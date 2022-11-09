# -*- coding: utf-8 -*-

import os
import json

class MinIOConfig:

    def __init__(self, conf_dir=None):
        try:
            if not conf_dir:
                conf_dir = os.getcwd()
            _conf_path = os.path.join(conf_dir, 'minio_config.json')
            self._config = json.load(open(_conf_path, "r"))
        except:
            self._config = {
                "main": {
                    "aws_access_key_id": "test",
                    "aws_secret_access_key": "test",
                    "endpoint_url": "http://test.iptime.org:1234"
                }
            }

    @property
    def main(self):
        return self._config.get('main')

    @property
    def bike(self):
        return self._config.get('bike')
