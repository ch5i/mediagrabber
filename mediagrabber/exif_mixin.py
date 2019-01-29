# To change this license header, choose License Headers in Project Properties.
# To change this template file, choose Tools | Templates
# and open the template in the editor.

import datetime
import logging
import re

from exiftool import ExifTool


class ExifMixin:
    """
    Mixin class used to add ExifTool related methods to a base class
    """

    def __init__(self, exiftool_process=None, logger=None, *args, **kwargs):
        self.logger = logger or logging.getLogger(__name__)
        self.logger.debug('Init ExifMixin')
        self.logger.debug('ET: %s', exiftool_process)

        super(ExifMixin, self).__init__(*args, **kwargs)

        # set up properties
        self._exiftool_process = exiftool_process
        self._external_et_process = False
        self.exif_data = {}

        # exif tags used to determine creation timestamp - the oldest date of these is considered the create date!
        self.exif_create_date_tags = [
            'EXIF:DateTimeOriginal',
            'EXIF:CreateDate',
            'QuickTime:CreateDate',
            'QuickTime:TrackCreateDate',
            'H264:DateTimeOriginal',
            'QuickTime:MediaCreateDate',
            'MediaCreateDate',
            'XMP:DateTime',
            'File:FileName',
            'EXIF:ModifyDate',
            'File:FileCreateDate',
            'File:FileModifyDate'
        ]

        # exif tag definitions
        self.exif_read_tags = [
            'Make',
            'Model',
            'GPSLatitude',
            'GPSLongitude',
            'ImageWidth',
            'ImageHeight'
        ]

        # combine tags
        self.exif_read_tags.extend(self.exif_create_date_tags)

        if exiftool_process is not None:
            # a handle to an externally started exiftool
            # process was passed, use it
            self._exiftool_process = exiftool_process
            self._external_et_process = True
        else:
            # start own exiftool process
            self.start_et_process()

    def __del__(self):
        self.logger.debug("Del ExifMixin")
        if not self._external_et_process:
            self.terminate_et_process()

    def start_et_process(self):
        """
        Starts local ExifTool process

        :return:
        """
        if not self._external_et_process and self._exiftool_process is None:
            self._exiftool_process = ExifTool()
            self._exiftool_process.start()
            self.logger.debug('started new et process')

    def terminate_et_process(self):
        """
        Terminates local ExifTool process

        :return:
        """
        if not self._external_et_process and self._exiftool_process is not None:
            self._exiftool_process.terminate()
            self.logger.debug('terminated et process')

    def read_exif_data(self, path_to_file):
        """
        Reads all exif tags from file

        :param path_to_file
        :return:
        """
        if self._exiftool_process is None:
            self.start_et_process()

        self.exif_data = {}
        self.exif_data = self._exiftool_process.get_metadata(path_to_file)
        print('Read exif data:\n', self.exif_data)

    def read_exif_tags(self, path_to_file):
        """
        Read specified exif tags from file

        Tags are specfied in self.exif_read_tags
        :param path_to_file
        :return:
        """

        if self._exiftool_process is None:
            self.start_et_process()

        self.exif_data = {}
        self.exif_data = self._exiftool_process.get_tags(self.exif_read_tags, path_to_file)
        self.logger.debug('Read tags: %s', self.exif_data)

    def parse_exif_tags(self, path_to_file):
        """
        Wrapper for read_exif_tags which additionally collapses the create date
        :param path_to_file:
        """
        self.read_exif_tags(path_to_file)
        self._exif_collapse_create_dates()
        self.logger.debug('Parsed tags: %s', self.exif_data)

    def _exif_collapse_create_dates(self):
        """
        Parses the exif_data and looks for the oldest timestamp

        in the create_date_tags. The timestamp is stored in a new tag
        called CollapsedDateTimeOriginal

        All tags matching a tag in create_date_tags are deleted
        """

        date_time_original = None
        oldest_date_time_original = None

        for tag in self.exif_create_date_tags:
            if tag in self.exif_data:
                date_time_original = self.exif_data[tag][:19]  # use only first 19chars
                if not self._is_valid_timestamp_format(date_time_original):
                    # as a last resort, try fixing timestamp format (i.e., if coming from filename)
                    date_time_original = self._try_fix_timestamp_format(date_time_original)
                    if not self._is_valid_timestamp_format(date_time_original):
                        date_time_original = None

                if date_time_original is not None:
                    if oldest_date_time_original is None:
                        oldest_date_time_original = date_time_original
                    else:
                        dto = self._get_date_from_timestamp(self, date_time_original)
                        odto = self._get_date_from_timestamp(self, oldest_date_time_original)
                        # if dto is before odto, reassign
                        if dto < odto:
                            oldest_date_time_original = date_time_original

                del self.exif_data[tag]  # remove all create date tags

        self.exif_data['CollapsedDateTimeOriginal'] = oldest_date_time_original
        self.logger.debug('collapsedDateTimeOriginal: %s', oldest_date_time_original)

        if not date_time_original:
            self.logger.error('Something went wrong, could not extract creation date...')
            self.logger.error('exif data: %s', self.exif_data)
            return False

        return True

    @staticmethod
    def _is_valid_timestamp_format(timestamp_str=''):
        """
        Helper method to validate timestamp format of string

        Validates timestamp format, must match format 'YYYY:mm:dd HH:mm:ss' as used in exif tags
        """
        if re.match(r'[1-9]\d{3}:[0-1]\d:[0-3]\d [0-2]\d:[0-5]\d:[0-5]\d', str(timestamp_str)):
            return True
        else:
            return False

    @staticmethod
    def _try_fix_timestamp_format(timestamp_str=''):
        """
        Helper method to try fixing timestamp format of string

        Tries to convert timestamp to format 'YYYY:mm:dd HH:mm:ss' as used in exif tags
        """
        timestamp_str = re.sub(r'([1-9]\d{3})[.:-]([0-1]\d)[.:-]([0-3]\d)[ _]([0-2]\d)[.:]([0-5]\d)[.:]([0-5]\d)',
                               r'\1:\2:\3 \4:\5:\6',
                               str(timestamp_str))

        return timestamp_str

    @staticmethod
    def _get_date_from_timestamp(cls, timestamp_str=''):
        """
        Helper method to convert an exif timestamp into a date

        Takes a string representation of a timestamp and returns a date object
        Date format YYYY:mm:dd HH:mm:ss
        :param timestamp_str:
        """
        date_obj = None

        if timestamp_str and cls._is_valid_timestamp_format(timestamp_str):
            date_obj = datetime.datetime.strptime(timestamp_str, "%Y:%m:%d %H:%M:%S")

        return date_obj


if __name__ == "__main__":
    print("Running MediaFile directly")
    em = ExifMixin()
    print('em:\n', em)
    del em
