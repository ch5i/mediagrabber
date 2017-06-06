# To change this license header, choose License Headers in Project Properties.
# To change this template file, choose Tools | Templates
# and open the template in the editor.

import argparse
import logging
import logging.handlers
import os
import re
import shutil
import sys
from collections import namedtuple
from timeit import default_timer as timer

from database import DataBase
from exifmediafile import ExifMediaFile
from exiftool import ExifTool


class MediaGrabber:
    """
    Used to aggregate media files (photos and videos) into a folder structure
    
    The target folder is structured according to the files creation timestamps
    The files are renamed according their creation timestamp
    
    A sqlite database is used to keep track of already imported files
    Use case is dropbox camera folders which are synchronized into a common
    folder structures
    """

    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.import_mode = True  # add sources and copy/move files
        self.simulate = False
        self.debug = False
        self.quiet = False
        self.move = False
        self.mode = 'import'
        self.source_dirs = []
        self.target_dir = ''
        self.ignore_subfolder_patterns = []
        self.file_extensions = []
        self.db_file = '.mediagrabber.db'
        self.location = os.path.dirname(os.path.abspath(__file__))

        # stats counters
        self.stats = namedtuple('stats',
                                ['file_count', 'db_count', 'skipped_files', 'removed_db_entries', 'total_time_file',
                                 'total_time_db'])
        self.stats.file_count = 0
        self.stats.db_count = 0
        self.stats.skipped_files = 0
        self.stats.removed_db_entries = 0
        self.stats.total_time_file = 0
        self.stats.total_time_db = 0

        # check config and arguments
        self._read_config()
        self._read_arguments()

        # set up loggers
        self._add_log_handler_console()
        self._add_log_handler_file_info()
        if self.debug:
            self._add_log_handler_file_debug()
        self.logger.debug("loggers started")

        self.logger.debug('Init MediaGrabber')

        self.logger.info('mediagrabber starting')

        # check encodings (i.e. fs enc. = utf-8?)
        self._check_encodings()

        # record run params in log
        self._log_run_parameters()

        # check directories
        self._check_target_dir()
        self._check_source_dirs()

        # set db file
        self.db_file = os.path.join(self.target_dir, self.db_file)

        # Initialize database
        self.db = DataBase(self.db_file)

        # dispatch according to mode
        self._dispatch()

        # clean up
        self.db.disconnect()

    def _check_target_dir(self):
        if self.target_dir is None or not os.path.exists(self.target_dir):
            error_no_target = 'cannot access target directory: <' + str(self.target_dir) + '> - exiting...'
            self.logger.error(error_no_target)
            sys.exit()

    def _check_source_dirs(self):
        if len(self.source_dirs) > 0:
            for source_dir in self.source_dirs:
                if source_dir is not None and not os.path.exists(source_dir):
                    self.logger.warning('source directory is not accessible: <' + source_dir + '>')

    def _check_encodings(self):
        """
        Check default- and filesystem encoding and show warning if fs encoding is not 'utf-8'
        """
        self.logger.debug('default encoding: %s', sys.getdefaultencoding())
        fs_encoding = sys.getfilesystemencoding()
        self.logger.debug('filesystem encoding: %s', fs_encoding)
        ok_fs_encodings = {
            'utf-8',
            'mbcs'
        }
        if fs_encoding.lower() not in ok_fs_encodings:
            self.logger.warning(
                "filesystem encoding '%s' detected - potential problems! Recommend switching to 'utf-8'.", fs_encoding)

    def _dispatch(self):
        """
        Dispatch program flow according to mode
        :return:
        """
        self.logger.info('')
        if self.mode == 'import':
            self.logger.info('-- importing --')
            self.logger.info('')
            if self.db.db_is_empty():
                self._rebuild_index()
            self._import_files()

        elif self.mode == 'index':
            self.logger.info('-- indexing --')
            self.logger.info('')
            self._rebuild_index()

        elif self.mode == 'reset':
            self._reset_sources()

        else:
            self.logger.error('unknown mode!')

    def _add_log_handler_console(self):
        """
        Add log handler for console output
        :return:
        """
        console_log_formatter = logging.Formatter('%(asctime)s  %(levelname)-8s  %(message)s',
                                                  datefmt='%Y-%m-%d %H:%M:%S')
        console_log_handler = logging.StreamHandler(sys.stdout)
        if self.quiet:
            console_log_handler.setLevel(logging.WARNING)
        else:
            console_log_handler.setLevel(logging.INFO)

        console_log_handler.setFormatter(console_log_formatter)
        self.logger.parent.addHandler(console_log_handler)

    def _add_log_handler_file_debug(self):
        """
        Add a lof file handler for log level 'debug'
        :return:
        """
        log_file = os.path.join(self.location, 'mediagrabber_debug.log')
        debug_file_log_formatter = logging.Formatter(
            '%(asctime)-s : %(levelname)-8s : %(filename)s (%(lineno)s) : %(funcName)s : %(message)s')
        debug_file_log_handler = logging.handlers.RotatingFileHandler(log_file, encoding='utf-8',
                                                                      maxBytes=10 * 1024 * 1024,
                                                                      backupCount=10)
        debug_file_log_handler.setLevel(logging.DEBUG)
        debug_file_log_handler.setFormatter(debug_file_log_formatter)
        self.logger.parent.addHandler(debug_file_log_handler)

    def _add_log_handler_file_info(self):
        """
        Add a lof file handler for log level 'info'
        :return:
        """
        # add file handler (info)
        log_file = os.path.join(self.location, 'mediagrabber.log')
        file_log_formatter = logging.Formatter('%(asctime)s  %(levelname)-8s %(message)s',
                                               datefmt='%Y-%m-%d %H:%M:%S')
        file_log_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
        file_log_handler.setLevel(logging.INFO)
        file_log_handler.setFormatter(file_log_formatter)
        self.logger.parent.addHandler(file_log_handler)

    def _log_run_parameters(self):
        """
        Show call parameters
        """
        self.logger.info('run parameters:')
        self.logger.info('> mode       = %s', self.mode)
        self.logger.info('> sources    = %s', self.source_dirs)
        self.logger.info('> target     = %s', self.target_dir)
        self.logger.info('> extensions = %s', self.file_extensions)
        self.logger.info('> ignored    = %s', self.ignore_subfolder_patterns)
        self.logger.info('> move       = %s', self.move)
        self.logger.info('> dryrun     = %s', self.simulate)
        self.logger.info('> quiet      = %s', self.quiet)

    def _read_config(self, config_file=None):
        """
        Read config settings from config file (into global vars)
        :param config_file:
        """

        # TODO: implement config file / remove entirely

        # if not config_file:
        #     config_file = '../mediagrabber_settings.py'

        # exec(open(config_file).read(), globals())

        self.source_dirs = ['./test']
        self.target_dir = './target'
        self.ignore_subfolder_patterns = [
            r'@eaDir',
            r'\.svn',
            r'__'
        ]

        self.file_extensions = [
            'jpg',
            'mov',
            'mts',
            'mp4'
        ]

        # name of the sqlite db file
        self.db_file = '.mediagrabber.db'

    def _read_arguments(self):
        """
        Parse commandline arguments
        """
        parser = argparse.ArgumentParser(description='A media grabber program')
        parser.add_argument('-m', '--mode', action='store', choices=('import', 'index', 'reset'), default='import',
                            dest='mode',
                            help=(
                                'import: import files from source dirs to target dir and index \n'
                                'index: validate/update target index \n'
                                'reset: reset sources: remove all source infos (but keep target index)'
                            ))
        parser.add_argument('-s', '--sourcedirs', nargs='*', action='store', dest='source_dirs',
                            help='directories to import from (use "" for names with spaces)')
        parser.add_argument('-t', '--targetdir', nargs='?', action='store', dest='target_dir',
                            help='directory to import to (indexed)')
        parser.add_argument('-e', '--extensions', nargs='*', action='store', default='jpg', dest='file_extensions',
                            help='list of file extensions to import (default: jpg)')
        parser.add_argument('-i', '--ignore-dirs', nargs='*', action='store', dest='ignore_dirs',
                            help='dirname patterns for subdirectories which should not be imported')
        parser.add_argument('-r', '--remove-sources', action='store_true', dest='move',
                            help='if this option is added, source files are moved to target (instead of copied)!')
        parser.add_argument('-p', '--probe', action='store_true', dest='sim',
                            help='probe: do no touch files - preview only')
        parser.add_argument('-q', '--quiet', action='store_true', dest='quiet', default=False,
                            help='quiet: no processing output to console')
        parser.add_argument('-d', '--debug', action='store_true', dest='debug',
                            help='debug: create detailed debug logfile')
        args = parser.parse_args()

        self.logger.debug('arguments:')
        self.logger.debug('> mode       = %s', args.mode)
        self.logger.debug('> sources    = %s', args.source_dirs)
        self.logger.debug('> target     = %s', args.target_dir)
        self.logger.debug('> extensions = %s', args.file_extensions)
        self.logger.debug('> ignored    = %s', args.ignore_dirs)
        self.logger.debug('> move       = %s', args.move)
        self.logger.debug('> dryrun     = %s', args.sim)
        self.logger.debug('> quiet      = %s', args.quiet)
        self.logger.debug('> debug      = %s', args.debug)

        self.mode = args.mode
        self.source_dirs = args.source_dirs
        self.target_dir = args.target_dir
        self.file_extensions = args.file_extensions
        self.ignore_subfolder_patterns = args.ignore_dirs
        self.move = args.move
        self.simulate = args.sim
        self.quiet = args.quiet
        self.debug = args.debug

        if not args.source_dirs:
            self.logger.info('> sources not specified - using source dirs from config')

        if not args.target_dir:
            self.logger.info('> target not specified - using target dir from config')

    @staticmethod
    def _filter_file_by_ext(the_file, filter_ext=None):
        """
        Filter the given file by the given extension
        Returns True if filter matches
        :param filter_ext:
        :param the_file:
        """
        if filter_ext is None:
            filter_ext = []

        filter_ext = [item.lower() for item in filter_ext]
        file_ext = os.path.splitext(the_file)[1][1:].strip().lower()

        if not filter_ext or (file_ext in filter_ext):
            return True
        else:
            return False

    def _filter_path(self, the_path, ignore_patterns=None):
        """
        Filter the given path against ignore patterns

        Returns True, if there is a match
        :param the_path: str
        :param ignore_patterns: [str]
        """
        if ignore_patterns is None:
            ignore_patterns = []

        self.logger.debug('path to filter: %s', the_path)
        self.logger.debug('path filter pattern: %s', ignore_patterns)

        for pattern in ignore_patterns:
            if re.search(pattern, the_path):
                self.logger.debug("found match for pattern '%s' in '%s'", pattern, the_path)
                self.logger.debug('excluded: %s', the_path)
                return True
            else:
                self.logger.debug("no match for pattern '%s' in '%s'", pattern, the_path)
                continue

        self.logger.debug('included: %s', the_path)
        return False

    def _rebuild_index(self):
        """
        Clear database and rebuild media index from scratch

        Searches given dirs and indexes files - this will clear all info on previous imports,
        so the next import run will need to make a full scan of the import folders

        :return:
        """
        # check if target file entriese in db are valid (drop invalid)
        self._validate_target_records()
        self.logger.info('')

        # check if all files in target have a record in db files table (add missing entries)
        self._scan_target_files()

    def _scan_target_files(self):
        """
        loop trough all target dir files and check if in db
        if not in db, import
        :return:
        """
        self.logger.info('checking for non-indexed files in target...')
        self.import_mode = False
        dir_list = [self.target_dir]
        self._process_files(dir_list)

    def _validate_target_records(self):
        """
        go through all target records and see if file is there
        if file does not exist, delete db record
        :return:
        """
        target_files_in_db = self.db.get_target_file_list()
        nof_db_files = len(target_files_in_db)

        if nof_db_files > 0:
            self.logger.info('validating ' + str(nof_db_files) + ' target records...')
            self.logger.info('---')

            file_count = 0
            removed_count = 0
            total_time = 0

            for db_file in target_files_in_db:
                start = timer()
                file_count += 1
                self.logger.debug(db_file)
                fn = os.path.join(self.target_dir, db_file['relative_path'], db_file['filename'])
                self.logger.info(
                    '[' + str(file_count) + ']: ' + db_file['filename'] + ' (id:' + str(db_file['file_id']) + ')')
                if not os.path.isfile(fn):
                    self.logger.info('file does not exist - dropping target record')
                    # delete record
                    self.db.drop_target_record(db_file['file_id'])
                    removed_count += 1
                else:
                    self.logger.info("ok - file exists in target")

                end = timer()
                processing_time = end - start
                total_time += processing_time

                self.logger.info('time: %ss / total: %ss', format(processing_time, '.3f'), format(total_time, '.2f'))
                self.logger.info('---')

            self.logger.info('...done, checked %s entries in %ss and removed %s entries', str(file_count),
                             format(total_time, '.3f'), str(removed_count))

            # update stats counters
            self.stats.total_time_db += total_time
            self.stats.db_count += file_count
            self.stats.removed_db_entries += removed_count

        else:
            self.logger.info('database contains no target records to validate')

    def _import_files(self):
        """
        Scan import directories and check files against index copy if new file

        In case the file is in the index but source info is empty update index source info.
        If file is not found in index, copy the file

        :return:
        """
        self.import_mode = True
        self.logger.info('start processing files...')
        self._process_files(self.source_dirs)

    def _process_files(self, list_of_dirs):
        """
        Process a list of files - check file name and content and copy/move accordingly
        :param list_of_dirs:
        """
        # start ExifTool
        et = ExifTool()
        et.start()

        # init stats counters
        total_time = 0
        file_count = 0
        skipped_count = 0

        self.logger.info('---')

        # iterate over source dirs
        for my_path in list_of_dirs:

            # check path
            if not os.path.exists(my_path):
                self.logger.warning('source directory "' + my_path + '" is not accessible!')
                self.logger.info('---')
                continue

            my_path = os.path.abspath(my_path)

            self.logger.info('getting list of files in "' + my_path + '" ...')
            file_list = self._get_file_list(my_path)

            nof_files = len(file_list)

            if nof_files > 0:
                self.logger.info('found ' + str(nof_files) + ' files to process (' + str(file_count + 1) + ' - ' + str(
                    file_count + nof_files) + ')')
            else:
                self.logger.info('directory contains no files for processing')

            self.logger.info('---')

            # iterate over source files and import new files to target
            for my_file in file_list:
                start = timer()
                file_count += 1
                emf = None

                self.logger.info('[' + str(file_count) + '] : ' + my_file)

                # check if source filename exists in db
                if self.import_mode is True and self.db.source_exists(my_file):
                    # is known source, skip
                    skipped_count += 1
                    self.logger.info('file is a known source - skipping')
                else:
                    # if not known: get file info (without md5, to save time)
                    emf = ExifMediaFile(my_file, et)
                    emf.parse_exif_info()

                    # check for matching target filename in db
                    if self.db.target_filename_matches(emf):
                        # if target fn matches:
                        # check if sizes match
                        if self.db.target_size_matches(emf):
                            # probably duplicate, calculate & check md5 to make sure
                            emf.calculate_md5()
                            if self.db.target_hash_matches(emf):
                                # md5 match: file is duplicate
                                self.logger.info(
                                    'file is already in target (' + emf.get_target_filename() + ')')
                                # add source entry for this file
                                if self.import_mode is True:
                                    self.logger.info('added as new source')
                                    self.db.add_source(emf)
                                    # skip (no file operation)
                                else:
                                    # count as skipped if indexing
                                    skipped_count += 1
                            else:
                                # file has different md5 (but same target name; i.e. photo taken in same second)
                                # insert with new target filename
                                self.db.assign_unique_target_filename(emf)
                                self.logger.warning('name collision - filename is already'
                                                    ' in target but with different content! (md5 mismatch)'
                                                    ' - inserting as new file - ' + emf.get_target_filename())
                                self._insert_new_target_file(emf)
                        else:
                            # file size does not match (but same target name; i.e. photo taken in same second)
                            # insert with new target filename
                            self.db.assign_unique_target_filename(emf)
                            self.logger.warning('name collision - filename is already'
                                                ' in target but with different content! (size mismatch)'
                                                ' - inserting as new file - ' + emf.get_target_filename())
                            self._insert_new_target_file(emf)
                    else:
                        # target filename does not yet exist
                        # check md5 to see if we have the file already (unlikely)
                        emf.calculate_md5()
                        if self.db.target_hash_matches(emf):
                            self.logger.warning('duplicate file content - content'
                                                ' in target but with different filename!'
                                                ' - inserting as new file - ' + emf.get_target_filename())
                        else:
                            self.logger.info('inserting new file - ' + emf.get_target_filename())

                        self._insert_new_target_file(emf)

                if emf is not None:
                    self.logger.debug('target name: %s, target size: %s', emf.get_target_filename(),
                                      emf.file_properties['file_size'])
                end = timer()
                processing_time = end - start
                total_time += processing_time

                self.logger.info('time: %ss / total: %ss', format(processing_time, '.3f'), format(total_time, '.2f'))
                self.logger.info('---')

        # update stats counters
        self.stats.total_time_file += total_time
        self.stats.file_count += file_count
        self.stats.skipped_files += skipped_count

        # clean up
        et.terminate()

        self.logger.info('...done!')
        self.logger.info('')

        # display stats
        self._show_stats()

    def _show_stats(self):
        # display some stats
        self.logger.info('processing stats:')
        self.logger.info('---')

        # files
        if self.stats.file_count > 0:
            self.logger.info('files')
            self.logger.info('> added            : %s', str(self.stats.file_count - self.stats.skipped_files))
            self.logger.info('> skipped          : %s', str(self.stats.skipped_files))
            self.logger.info('> total            : %s', str(self.stats.file_count))
            self.logger.info('> avg. time/file   : %ss',
                             format(self.stats.total_time_file / self.stats.file_count, '.3f'))
            self.logger.info('> total time       : %ss', format(self.stats.total_time_file, '.2f'))
            self.logger.info('---')

        # db records (validation)
        if self.stats.db_count > 0:
            self.logger.info('target records')
            self.logger.info('> validated        : %s', str(self.stats.db_count))
            self.logger.info('> removed          : %s', str(self.stats.removed_db_entries))
            self.logger.info('> avg. time/record : %ss', format(self.stats.total_time_db / self.stats.db_count, '.3f'))
            self.logger.info('> total time       : %ss', format(self.stats.total_time_db, '.2f'))
            self.logger.info('---')

        # total time
        if self.stats.db_count > 0 and self.stats.file_count > 0:
            self.logger.info('overall')
            self.logger.info('> total objects     : %s',
                             str(self.stats.file_count + self.stats.db_count))
            self.logger.info('> total time        : %ss',
                             format(self.stats.total_time_file + self.stats.total_time_db, '.2f'))
            self.logger.info('---')

    def _insert_new_target_file(self, emf):

        # make sure we have md5 hash of file
        if emf.file_properties['file_hash_md5'] is None:
            emf.calculate_md5()

        # add db record for file
        self.db.add_file(emf)

        # add sources and move files if import mode is on
        if self.import_mode is True:

            self.db.add_source(emf)

            # move/copy physical file
            source = os.path.abspath(emf.get_full_source_path())
            target_path = os.path.abspath(os.path.join(self.target_dir, emf.get_target_path()))
            target = os.path.join(target_path, emf.get_target_filename())

            # just a dry run?
            if not self.simulate:
                if not os.path.exists(target_path):
                    os.makedirs(target_path)
                    self.logger.debug('created  dir <' + target_path + '>')

                if os.path.isfile(target):
                    self.logger.warning('physical file <' + target + '> already exists in target!')
                else:

                    if self.move is True:
                        shutil.move(source, target)
                        self.logger.info('moved file <' + source + '> to <' + target + '>')

                        # TODO: remove directory, if it is empty after moving out the file

                    else:
                        shutil.copy(source, target)
                        self.logger.info('copied file <' + source + '> to <' + target + '>')

                    # TODO: Add return value (success/fail)
                    self.db.update_copy_flags(emf)
            else:
                self.logger.info('simulated copy/move of file <' + source + '> to <' + target + '>')

    def _reset_sources(self):
        """
        remove all source infos (i.e. drop all records from source table)

        :return:
        """
        self.db.drop_sources()
        self.logger.info('dropped all source infos')

    def _get_file_list(self, the_path):
        """
        Build a list of files in the passed directory which matches the extensions
        passed in filter_extensions. Subdirs which match ignore_subfolder_patterns are ignored
        :param the_path: str
        :return: []
        """

        file_list = []

        for root, dirs, files in os.walk(the_path):

            # filter out paths with matches in the list of ignore patterns
            dirs[:] = [d for d in dirs if
                       not self._filter_path(os.path.join(root, d), self.ignore_subfolder_patterns)]

            # loop through list and add files if extension matches
            for fn in files:
                fn = os.path.join(root, fn)
                if os.path.isfile(fn):
                    if self._filter_file_by_ext(fn, self.file_extensions):
                        self.logger.debug('added file: %s', fn)
                        file_list.append(fn)

        return file_list


def init_loggers():
    # get logger and set level (required)
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)


def main():
    # override fs encoding
    init_loggers()
    mg = MediaGrabber()


if __name__ == "__main__":
    main()
