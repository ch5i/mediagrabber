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
        self.indexing_mode = False  # default mode is import, not indexing
        self.simulate = False
        self.logfile_name = ''
        self.debug = False
        self.quiet = False
        self.verbose = False
        self.move = False
        self.mode = 'import'
        self.source_dirs = []
        self.target_dir = ''
        self.ignore_subfolder_patterns = []
        self.file_extensions = []
        self.db_file = '.mediagrabber.db'
        self.location = os.path.dirname(os.path.abspath(__file__))

        # initialize stats
        self._init_stats()

        # check arguments
        self._read_arguments()

        self._setup_loggers()

        self.logger.debug('Init MediaGrabber')

        self.logger.info('mediagrabber starting')
        self.logger.info('')

        # record run params in log
        self._log_run_parameters()

        # check encodings (i.e. fs enc. = utf-8?)
        self._check_encodings()

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

    def _init_stats(self):
        # set up stats counters
        self.stats = namedtuple('stats',
                                ['file_count', 'total_file_size', 'db_count', 'skipped_files', 'removed_db_entries',
                                 'total_time_file', 'total_time_db'])
        self.stats.file_count = 0
        self.stats.total_file_size = 0
        self.stats.db_count = 0
        self.stats.skipped_files = 0
        self.stats.removed_db_entries = 0
        self.stats.total_time_file = 0
        self.stats.total_time_db = 0

    def _setup_loggers(self):
        # set up loggers
        self._add_log_handler_console()

        if self.logfile_name is not None:
            self._add_log_handler_file()

        if self.debug:
            self._add_log_handler_file_debug()
        self.logger.debug("loggers set up")

    def _check_target_dir(self):
        if self.target_dir is None or not os.path.exists(self.target_dir):
            error_no_target = 'cannot access target directory: <' + str(self.target_dir) + '> - exiting...'
            self.logger.error(error_no_target)
            sys.exit(error_no_target)

    def _check_source_dirs(self):
        if self.source_dirs is not None and len(self.source_dirs) > 0:
            for source_dir in self.source_dirs:
                if source_dir is not None and not os.path.exists(source_dir):
                    self.logger.warning('source directory is not accessible: <' + source_dir + '>')
        else:
            no_sources = 'no source directories specified!'
            if self.mode == 'import':
                no_sources = no_sources + ' - exiting...'
                self.logger.error(no_sources)
                sys.exit(no_sources)
            else:
                self.logger.debug(no_sources)

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

    def _add_log_handler_file(self):
        """
        Add a lof file handler for log level 'info'
        :return:
        """
        # add file handler (info)
        if os.path.basename(self.logfile_name) == self.logfile_name:
            self.logfile_name = os.path.join(self.location, self.logfile_name)
        else:
            if not os.path.exists(os.path.dirname(self.logfile_name)):
                os.makedirs(os.path.dirname(self.logfile_name))

        file_log_formatter = logging.Formatter('%(asctime)s  %(levelname)-8s %(message)s',
                                               datefmt='%Y-%m-%d %H:%M:%S')
        file_log_handler = logging.FileHandler(self.logfile_name, mode='w', encoding='utf-8')
        file_log_handler.setLevel(logging.INFO)
        file_log_handler.setFormatter(file_log_formatter)
        self.logger.parent.addHandler(file_log_handler)

    def _log_run_parameters(self):
        """
        Show call parameters
        """
        self.logger.info('run parameters:')
        self.logger.info('---')
        self.logger.info('> mode       = %s', self.mode)
        self.logger.info('> sources    = %s', self.source_dirs)
        self.logger.info('> target     = %s', self.target_dir)
        self.logger.info('> extensions = %s', self.file_extensions)
        self.logger.info('> ignored    = %s', self.ignore_subfolder_patterns)
        self.logger.info('> move       = %s', self.move)
        self.logger.info('> dryrun     = %s', self.simulate)
        self.logger.info('> logfile    = %s', self.logfile_name)
        self.logger.info('> verbose    = %s', self.verbose)
        self.logger.info('> quiet      = %s', self.quiet)
        self.logger.info('> debug      = %s', self.debug)
        self.logger.info('---')

    def _read_arguments(self):
        """
        Parse commandline arguments
        """
        parser = argparse.ArgumentParser(description='A media grabber program')
        parser.add_argument('-m', '--mode', choices=('import', 'index', 'reset'), default='import',
                            dest='mode',
                            help=(
                                'import: import files from source dirs to target dir and index \n'
                                'index: validate/update target index \n'
                                'reset: reset sources: remove all source infos (but keep target index)'
                            ))
        parser.add_argument('-s', '--sourcedirs', nargs='+', dest='source_dirs',
                            help='directories to import from (use "" for names with spaces)')
        parser.add_argument('-t', '--targetdir', dest='target_dir',
                            help='directory to import to (indexed)')
        parser.add_argument('-e', '--extensions', nargs='+', default='jpg', dest='file_extensions',
                            help='list of file extensions to import (default: jpg)')
        parser.add_argument('-i', '--ignore-dirs', nargs='+', dest='ignore_dirs',
                            help='dirname patterns for subdirectories which should not be imported')
        parser.add_argument('-r', '--remove-sources', action='store_true', default=False, dest='move',
                            help='if this option is added, source files are moved to target (instead of copied)!')
        parser.add_argument('-p', '--probe', action='store_true', default=False, dest='sim',
                            help='probe: do no touch files - preview only')
        parser.add_argument('-l', '--logfile', nargs='?', dest='logfile', const='mediagrabber.log',
                            help='write logfile (optional: specify logfile)')
        parser.add_argument('-v', '--verbose', action='store_true', default=False, dest='verbose',
                            help='verbose: output more information, e.g. skipped files')
        parser.add_argument('-q', '--quiet', action='store_true', default=False, dest='quiet',
                            help='quiet: suppress all output to console')
        parser.add_argument('-d', '--debug', action='store_true', dest='debug',
                            help='debug: write a detailed logfile for debugging')
        args = parser.parse_args()

        self.mode = args.mode
        self.source_dirs = args.source_dirs
        self.target_dir = args.target_dir
        self.file_extensions = args.file_extensions
        self.ignore_subfolder_patterns = args.ignore_dirs
        self.move = args.move
        self.simulate = args.sim
        self.logfile_name = args.logfile
        self.quiet = args.quiet
        self.verbose = args.verbose
        self.debug = args.debug

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
        # check if target file entries in db are valid (drop invalid)
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
        self.indexing_mode = True
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
            self._selective_logger('---')

            file_count = 0
            removed_count = 0
            total_time = 0

            for db_file in target_files_in_db:
                start = timer()
                file_count += 1
                self.logger.debug(db_file)
                fn = os.path.join(self.target_dir, db_file['relative_path'], db_file['filename'])

                self._selective_logger(
                    '[' + str(file_count) + ']: ' + db_file['filename'] + ' (id:' + str(db_file['file_id']) + ')')

                if not os.path.isfile(fn):
                    self.logger.warning("file '" + db_file['filename'] + "' does not exist - dropping target record")
                    # delete record
                    self.db.drop_target_record(db_file['file_id'])
                    removed_count += 1
                else:
                    self._selective_logger("file exists in target - record is valid")

                end = timer()
                processing_time = end - start
                total_time += processing_time

                self._selective_logger('time: %ss / total: %ss', format(processing_time, '.3f'),
                                       format(total_time, '.2f'))
                self._selective_logger('---')

            self.logger.info('...done')
            self._selective_logger('checked %s entries in %ss and removed %s entries', str(file_count),
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
        self.indexing_mode = False
        self.logger.info('start processing files...')
        self._process_files(self.source_dirs)

    def _selective_logger(self, *args, **kwargs):
        """
        wrapper for self.logger to limit amount of log messages if not in verbose mode
        :param args: pass on arguments
        :param kwargs: pass on keyword arguments
        :return:
        """
        log_level = 'debug'
        if self.verbose is True:
            log_level = 'info'
        getattr(self.logger, log_level)(*args, **kwargs)

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
        total_files = 0
        file_count = 0
        skipped_count = 0
        last_times = []
        avg_time = 0
        min_timer_samples = 10

        # init file list
        file_list = []

        self._selective_logger('---')

        # iterate over source dirs
        for my_path in list_of_dirs:

            # check path
            if not os.path.exists(my_path):
                self.logger.warning('source directory "' + my_path + '" is not accessible!')
                self.logger.info('---')
                continue

            my_path = os.path.abspath(my_path)

            self._selective_logger('getting list of files in "' + my_path + '" ...')
            cur_dir_file_list = self._get_file_list(my_path)

            nof_files = len(cur_dir_file_list)

            if nof_files > 0:
                file_list.extend(cur_dir_file_list)
                total_files += nof_files
                self._selective_logger(
                    'found ' + str(nof_files) + ' files to process')

            else:
                self.logger.info('directory contains no files for processing')

            self._selective_logger('---')

        if len(file_list) == 0:
            # nothing to do
            self.logger.info('no files for process!')
        else:
            # iterate over source files and import new files to target
            for my_file in file_list:
                start = timer()
                file_count += 1
                total_file_size_before = self.stats.total_file_size
                emf = None

                self._selective_logger(
                    '[' + str(file_count) + ' / ' + str(total_files) + '] (' + format((file_count / total_files),
                                                                                      '.0f') + '%): ' + my_file)

                # check if source filename exists in db
                if self.db.source_exists(my_file) and self.indexing_mode is False:
                    # is known source, skip
                    skipped_count += 1
                    self._selective_logger('file is a known source - skipping')
                else:
                    # if not known: get file info
                    emf = ExifMediaFile(my_file, et)
                    emf.parse_exif_info()
                    emf.calculate_md5()

                    # check if hash matches
                    if self.db.file_hash_matches(emf):
                        # md5 match: file is duplicate
                        # count as skipped
                        skipped_count += 1

                        db_path, db_fn = self.db.get_target_path_filename(emf)

                        if self.indexing_mode is True:
                            # check if this is the file which is already in the db - else delete (duplicate)
                            if self._is_target_file(emf, my_file):
                                self._selective_logger("ok, record for file exists: '" + db_fn + "'")
                            else:
                                # file is a duplicate, remove
                                self.logger.warning(
                                    "duplicate found: original '" + db_fn + "' => removing duplicate: '" + my_file + "'")
                                self._remove_file(my_file)
                        else:
                            # add source entry for this file
                            self._selective_logger("file is already in target as '" + db_fn + "'")
                            self._selective_logger('added as new source')
                            self.db.add_source(emf)
                            # skip (no file operation)
                    else:
                        # md5 is different,
                        # insert file
                        self._selective_logger('identified as new file')
                        self._insert_new_target_file(emf)
                        self._selective_logger('created new file record: ' + emf.get_target_filename())

                if emf is not None:
                    self.logger.debug('target name: %s, target size: %s', emf.get_target_filename(),
                                      emf.file_properties['file_size'])
                end = timer()
                processing_time = end - start
                processing_size_mb = (self.stats.total_file_size - total_file_size_before) / 1024 / 1024
                total_time += processing_time

                # update average time
                if len(last_times) > min_timer_samples:
                    last_times.pop(0)  # remove oldest value
                last_times.append(processing_time)
                avg_time = sum(last_times) / float(len(last_times))

                # show some stats in verbose mode
                self._selective_logger('time: %ss / avg: %s | total: %ss | size: %sMB | remaining: %s files / ~%ss',
                                       format(processing_time, '.3f'),
                                       format(avg_time, '.3f'),
                                       format(total_time, '.2f'),
                                       format(processing_size_mb, '.2f'),
                                       str(total_files - file_count),
                                       format((total_files - file_count) * avg_time, '.0f')
                                       )
                self._selective_logger('---')

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

    def _is_target_file(self, emf: ExifMediaFile, my_file):

        db_path, db_fn = self.db.get_target_path_filename(emf)

        if os.path.normpath(os.path.join(self.target_dir, db_path, db_fn)) == os.path.normpath(my_file):
            # is target
            return True

        else:
            # is not target file
            return False

    def _show_stats(self):

        if self.stats.db_count > 0 or self.stats.file_count > 0:
            # display some stats
            self.logger.info('processing stats:')
            self.logger.info('---')

            # files
            if self.stats.file_count > 0:
                file_size_mb = self.stats.total_file_size / 1024 / 1024
                self.logger.info('files')
                self.logger.info('> total            : %s', str(self.stats.file_count))
                self.logger.info('> added            : %s', str(self.stats.file_count - self.stats.skipped_files))
                self.logger.info('> skipped          : %s', str(self.stats.skipped_files))
                self.logger.info('> added size       : %sMB', format(file_size_mb, '.2f'))
                self.logger.info('> avg. time/file   : %ss',
                                 format(self.stats.total_time_file / self.stats.file_count, '.3f'))
                self.logger.info('> total time       : %ss', format(self.stats.total_time_file, '.2f'))
                self.logger.info('---')

            # db records (validation)
            if self.stats.db_count > 0:
                self.logger.info('target records')
                self.logger.info('> validated        : %s', str(self.stats.db_count))
                self.logger.info('> removed          : %s', str(self.stats.removed_db_entries))
                self.logger.info('> avg. time/record : %ss',
                                 format(self.stats.total_time_db / self.stats.db_count, '.3f'))
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

    def _insert_new_target_file(self, emf: ExifMediaFile):

        # make sure we have md5 hash of file
        if emf.file_properties['file_hash_md5'] is None:
            emf.calculate_md5()

        # make sure filename is unique
        self.db.assign_unique_target_filename(emf)

        # add db record for file
        self.db.add_file(emf)

        # add sources
        if self.indexing_mode is False:
            self.db.add_source(emf)

        # move/copy physical file
        source = os.path.abspath(emf.get_full_source_path())
        target_path = os.path.abspath(os.path.join(self.target_dir, emf.get_target_path()))
        target = os.path.join(target_path, emf.get_target_filename())

        # dry run?
        if not self.simulate:
            if not os.path.exists(target_path):
                os.makedirs(target_path)
                self.logger.debug('created  dir <' + target_path + '>')

            if source != target:

                if os.path.isfile(target):
                    self.logger.info('physical file <' + source + '> already exists in target:  <' + target + '>!')

                    # clean up if move
                    if self.move is True:
                        os.remove(source)
                        self.logger.info('removed source file <%s>', source)
                        self._remove_dir_if_empty(os.path.dirname(source))

                    if self.indexing_mode is True:
                        # target file exists, we're on a copy.
                        self.logger.info('found extra copy: <' + source + '>')
                        # TODO: add option to prune extra copies
                else:

                    self.stats.total_file_size += emf.file_properties['file_size']

                    if self.move is True or self.indexing_mode is True:
                        shutil.move(source, target)
                        self.logger.info('moved file to <' + target + '>')
                        self._remove_dir_if_empty(os.path.dirname(source))
                    else:
                        shutil.copy(source, target)
                        self.logger.info('copied file to <' + target + '>')

                    # TODO: Add return value (success/fail)
                    self.db.update_copy_flags(emf)
        else:
            if source != target:
                filemode = 'copy'
                if self.move is True:
                    filemode = 'move'
                self._selective_logger('simulated ' + filemode + ' of file to <' + target + '>')

    def _remove_file(self, file_path):
        # dry run?
        if not self.simulate:
            os.remove(file_path)
            self._selective_logger('removed file <' + file_path + '>')
            self._remove_dir_if_empty(os.path.dirname(file_path))
        else:
            self._selective_logger('simulated delete of file <' + file_path + '>')

    def _remove_dir_if_empty(self, source_dir):
        if not self.simulate:
            parent_dir = os.path.abspath(os.path.join(source_dir, os.pardir))
            if not os.listdir(source_dir):
                os.rmdir(source_dir)
                self._selective_logger('removed empty source directory <%s>', source_dir)
                # recursively call again to remove empty dirs up to first non-empty parent dir
                self._remove_dir_if_empty(parent_dir)

    def _reset_sources(self):
        """
        remove all source infos (i.e. drop all records from source table)

        :return:
        """
        self.db.drop_sources()
        self.logger.info('done - dropped all source infos')

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
        # sort file list
        file_list.sort()
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
