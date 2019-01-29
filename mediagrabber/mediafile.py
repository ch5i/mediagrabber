# To change this license header, choose License Headers in Project Properties.
# To change this template file, choose Tools | Templates
# and open the template in the editor.

import datetime
import logging
import os

import filehash


class MediaFile:
    def __init__(self, file_path=None, logger=None, *args, **kwargs):
        self.logger = logger or logging.getLogger(__name__)
        self.logger.debug('Init MediaFile')
        self.logger.debug('Path: %s', file_path)

        super(MediaFile, self).__init__(*args, **kwargs)

        # set up properties
        self.file_id = None
        self.file_properties = {
            'file_hash_md5': None,
            'file_type': None,
            'file_size': None,
            'file_date': None
        }
        self.source_properties = {
            'file_id': None,
            'source_path': None,
            'source_filename': None,
        }

        self.full_path = None

        if file_path is not None:
            file_path = os.path.abspath(file_path)

            if os.path.exists(file_path):
                self.full_path = file_path
                self.source_properties['source_path'] = os.path.dirname(self.full_path)
                self.source_properties['source_filename'] = os.path.basename(self.full_path)
                self.file_properties['file_size'] = self.get_file_size()
                self.file_properties['file_date'] = self.get_file_date()
                self.file_properties['file_type'] = self.get_filetype()

    def __iter__(self):
        for attr, value in self.file_properties.items():
            yield attr, value

    def __str__(self):
        return str(dict(self))

    def name(self):
        return str(self.source_properties['source_filename'])

    def calculate_md5(self):
        if self.full_path is not None:
            md5 = filehash.md5_for_file(self.full_path)
            self.file_properties['file_hash_md5'] = md5
            return md5

    def get_filetype(self):
        if self.full_path is not None:
            file_name, file_ext = os.path.splitext(self.full_path)
            return file_ext.replace('.', '').upper()

    def get_file_date(self):
        if self.full_path is not None:
            c_date = os.path.getctime(self.full_path)
            c_date = datetime.datetime.fromtimestamp(c_date)
            c_date = "{:%Y-%m-%d %H:%M:%S}".format(c_date)
            return c_date

    def get_file_size(self):
        size = os.path.getsize(self.full_path)  # bytes
        return size

    def get_full_source_path(self):
        source_path = self.full_path
        return source_path

if __name__ == "__main__":
    print("Running MediaFile directly")
    mf = MediaFile('./test/sample_image.jpg')
    print(mf)
