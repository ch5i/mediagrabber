# To change this license header, choose License Headers in Project Properties.
# To change this template file, choose Tools | Templates
# and open the template in the editor.

import logging
import os

from exif_mixin import ExifMixin
from mediafile import MediaFile


class ExifMediaFile(ExifMixin, MediaFile):
    def __init__(self, file_path='', exiftool_process=None, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.logger.debug('Init ExifMediaFile')
        self.logger.debug('Path: %s', file_path)
        self.logger.debug('ET: %s', exiftool_process)
        super(ExifMediaFile, self).__init__(file_path=file_path, exiftool_process=exiftool_process)
        self.file_properties['date_time_original'] = None
        self.file_properties['target_path'] = None
        self.file_properties['target_filename'] = None
        # self.file_properties['image_height'] = None
        # self.file_properties['image_width'] = None
        # self.file_properties['camera_make'] = None
        # self.file_properties['camera_model'] = None
        # self.file_properties['gps_latitude'] = None
        # self.file_properties['gps_longitude'] = None

    def read_exif_data(self, file_path=None):
        super().read_exif_data(self.full_path)

    def read_exif_tags(self, file_path=None):
        """
        Utility method to read selected exif tags from file

        Exif tag info will not be added to self.file_properties
        :param file_path:
        :return:
        """
        super().read_exif_tags(self.full_path)

    def parse_exif_info(self):
        # read and parse exif info into self.file_properties
        super().parse_exif_tags(self.full_path)

        # date_time_original
        dto = self._get_date_from_timestamp(self, self.exif_data['CollapsedDateTimeOriginal'])
        self.file_properties['date_time_original'] = "{:%Y-%m-%d %H:%M:%S}".format(dto)

        # target_path
        self.file_properties['target_path'] = self._get_folder_path_from_date(dto)

        # target_filename
        target_filename = self._get_base_filename_from_date(dto).lower()
        target_extension = self.file_properties['file_type'].lower()
        target_filename = target_filename + '.' + target_extension
        self.file_properties['target_filename'] = target_filename

        if 'File:ImageHeight' in self.exif_data:
            self.file_properties['image_height'] = self.exif_data['File:ImageHeight']

        if 'File:ImageWidth' in self.exif_data:
            self.file_properties['image_width'] = self.exif_data['File:ImageWidth']

        if 'EXIF:Make' in self.exif_data:
            self.file_properties['camera_make'] = self.exif_data['EXIF:Make']

        if 'EXIF:Model' in self.exif_data:
            self.file_properties['camera_model'] = self.exif_data['EXIF:Model']

        if 'EXIF:GPSLatitude' in self.exif_data:
            self.file_properties['gps_latitude'] = self.exif_data['EXIF:GPSLatitude']

        if 'EXIF:GPSLongitude' in self.exif_data:
            self.file_properties['gps_longitude'] = self.exif_data['EXIF:GPSLongitude']

        self.logger.debug('file properties: %s', self.file_properties)

    def get_base_target_filename(self):
        if self.file_properties['target_filename'] is not None:
            base_file_name, file_ext = os.path.splitext(self.file_properties['target_filename'])
            base_file_name = base_file_name.lower()
        else:
            base_file_name = None
        return base_file_name

    def get_target_filename(self):
        if self.file_properties['target_filename'] is not None:
            filename = self.file_properties['target_filename'].lower()
        else:
            filename = None
        return filename

    def get_target_path(self):
        return self.file_properties['target_path']

    @classmethod
    def _get_folder_path_from_date(cls, date_obj=None):
        """
        Creates a partial folder path (string) from a date object
        """
        the_path = ''
        if type(date_obj) == str:
            date_obj = cls._get_date_from_timestamp(date_obj)

        if date_obj is not None:
            the_path = "{:%Y/%Y-%m/%Y-%m-%d}".format(date_obj)
        return the_path

    @classmethod
    def _get_base_filename_from_date(cls, date_obj=None):
        """
        Creates a filename string from a date object
        """
        if type(date_obj) == str:
            date_obj = cls._get_date_from_timestamp(date_obj)

        filename = ''
        if date_obj is not None:
            filename = "{:%Y-%m-%d %H.%M.%S}".format(date_obj)
        return filename

if __name__ == "__main__":
    print("Running ExifMediaFile directly")
    emf = ExifMediaFile('./test/sample_image.jpg')
    print('emf:\n', emf)
    emf.parse_exif_info()
    print('emf (exif):\n', emf)
    emf.calculate_md5()
    print('emf (exif + md5):\n', emf)
    del emf
