# To change this license header, choose License Headers in Project Properties.
# To change this template file, choose Tools | Templates
# and open the template in the editor.

import logging
import os
import sqlite3

from exifmediafile import ExifMediaFile

if __name__ == "__main__":
    print("MediaGrabber DB")


class DataBase:
    def __init__(self, path_to_db, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.logger.debug('Init DB')
        self.logger.debug('database: %s', path_to_db)
        self.simulate = False
        self.path_to_db = path_to_db
        self.db_connection = None
        self.connect()

    def __del__(self):
        self.disconnect()
        self.logger.debug('Exit DB')

    def connect(self):
        """
        Connect to the database. If it doesn't exist, create it. Enable foreign key support.
        """
        setup_db = False

        if self.path_to_db is not None:
            try:
                self.path_to_db = os.path.abspath(self.path_to_db)

                if not os.path.exists(self.path_to_db):
                    self.logger.warning('database not found:  %s', self.path_to_db)
                    setup_db = True

                self.logger.debug('connecting db')
                self.db_connection = sqlite3.connect(self.path_to_db)

                # enable foreign key support
                self.execute_sql('pragma foreign_keys = ON')

                if setup_db:
                    self.logger.info('setting up fresh database: %s', self.path_to_db)
                    self.setup_db()

            except Exception as error:
                self.logger.error("Oops, Didn't work: %s", error)

    def disconnect(self):
        if self.db_connection:
            self.logger.debug('disconnect db')
            self.db_connection.close()

    def setup_db(self):
        """
        Set up initial db structure (tables)
        """
        if self.db_connection is not None:
            # need to switch of simulation mode temporarily off if set
            simulate = self.simulate
            self.simulate = False

            # create db structure
            self._create_table_file()
            self._create_table_source()

            # restore simulation mode
            self.simulate = simulate

    def _create_table_source(self):
        """
        Create the table "source"
        """
        self.logger.debug('creating source table')
        structure_query = (
            'CREATE TABLE source ('
            'source_id INTEGER PRIMARY KEY NOT NULL UNIQUE,'
            'source_path TEXT NOT NULL,'
            'source_filename TEXT NOT NULL,'
            'file_id INTEGER NOT NULL'
            ' REFERENCES file (file_id) '
            ' ON DELETE CASCADE'
            ' ON UPDATE CASCADE,'
            'date_added TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP));'
        )
        self.execute_sql(structure_query)

        self.logger.debug('creating indexes')
        index_query = (
            'CREATE UNIQUE INDEX unique_path_filename ON source (source_path, source_filename);'
        )
        self.execute_sql(index_query)

    def _create_table_file(self):
        """
        Create the table "file"
        """
        self.logger.debug('creating file table')
        structure_query = (
            'CREATE TABLE file ('
            'file_id INTEGER PRIMARY KEY NOT NULL,'
            'file_type TEXT,'
            'file_size BIGINT,'
            'file_hash_md5 TEXT DEFAULT (NULL),'
            'file_date TIMESTAMP,'
            'date_time_original TIMESTAMP NOT NULL,'
            'target_path TEXT NOT NULL DEFAULT (NULL),'
            'target_filename TEXT NOT NULL DEFAULT (NULL),'
            'image_width TEXT DEFAULT (NULL),'
            'image_height TEXT DEFAULT (NULL),'
            'camera_make TEXT DEFAULT (NULL),'
            'camera_model TEXT DEFAULT (NULL),'
            'gps_longitude TEXT DEFAULT (NULL),'
            'gps_latitude TEXT DEFAULT (NULL),'
            'date_added TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP),'
            'copied BOOLEAN NOT NULL DEFAULT (0),'
            'date_copied TIMESTAMP);'
        )
        self.execute_sql(structure_query)

        self.logger.debug('creating indexes')
        index_query = (
            'CREATE UNIQUE INDEX idx_unique_target_path_file ON file (target_path, target_filename);'
        )
        self.execute_sql(index_query)

        index_query = (
            'CREATE UNIQUE INDEX idx_file_hash_md5 ON file (file_hash_md5);'
        )
        self.execute_sql(index_query)

    def execute_sql(self, sql_str):
        """
        Run a SQL statement against the database
        :param sql_str:
        """
        exec_result = None

        c = self.db_connection.cursor()

        try:
            self.logger.debug("executing statement: %s", sql_str)
            exec_result = c.execute(sql_str)

            if self.simulate:
                self.logger.debug('Simulation mode active => Rollback!')
                self.db_connection.rollback()
            else:
                self.db_connection.commit()

        except Exception as error:
            self.logger.error("Didn't work: %s => %s", sql_str, error, exc_info=True)
            raise error

        finally:
            if not exec_result:
                self.logger.error('Rollback')
                self.db_connection.rollback()

        return exec_result

    def drop_all_records(self):
        """
        Empty all tables
        source should be empty after deleting target records (foreign key action...)
        """
        sql = "DELETE FROM file"
        self.execute_sql(sql)
        sql = "DELETE FROM source"
        self.execute_sql(sql)

    def db_is_empty(self):
        """
        Check if there are any records in the DB
        :return: bool
        """
        sql = 'SELECT file_id FROM file'

        db_result = self.execute_sql(sql)
        data = db_result.fetchone()
        if data is None:
            self.logger.warning('DB is empty!')
            return True
        else:
            self.logger.debug('DB is not empty')
            return False

    def drop_sources(self):
        """
        Delete all sources and reset all copied and date_copied fields
        :return: 
        """
        # reset copy flags and dates
        sql = (
            "UPDATE file "
            "SET "
            "copied = 0,"
            "date_copied=''"
        )
        self.execute_sql(sql)

        # drop all sources
        sql = "DELETE FROM source"
        self.execute_sql(sql)

    def source_exists(self, source_file_path):
        """
        Check if source file path has match in db

        :param source_file_path:
        :return: bool
        """

        source_path = os.path.dirname(source_file_path)
        source_filename = os.path.basename(source_file_path)

        sql = (
            'SELECT file_id FROM source '
            'WHERE '
            "source_path = '{0}' "
            'AND '
            "source_filename = '{1}'"
        ).format(source_path, source_filename)

        db_result = self.execute_sql(sql)
        data = db_result.fetchone()
        if data is None:
            self.logger.debug('source not in db: %s', source_file_path)
            return False
        else:
            self.logger.debug('source exists in db: %s', source_file_path)
            return True

    def target_filename_matches(self, exif_media_file: ExifMediaFile):
        """
        Check if target filename has match in db

        :param exif_media_file:
        :return: bool
        """

        assert isinstance(exif_media_file, ExifMediaFile)

        target_file_name = exif_media_file.file_properties['target_filename']

        sql = (
            'SELECT file_id FROM file '
            'WHERE '
            "target_filename like '{0}%' "
        ).format(target_file_name)

        db_result = self.execute_sql(sql)
        db_data = db_result.fetchone()
        if db_data is None:
            exif_media_file.file_id = None
            self.logger.debug('target filename not in db: %s', target_file_name)
            return False
        else:
            exif_media_file.file_id = str(db_data[0])
            self.logger.debug('target filename exists db: %s (id: %s)', target_file_name, exif_media_file.file_id)
            return True

    def file_date_type_matches(self, exif_media_file: ExifMediaFile):
        """
        Check if capture time and file type match in db

        :param exif_media_file:
        :return: bool
        """

        assert isinstance(exif_media_file, ExifMediaFile)

        date_time_original = exif_media_file.file_properties['date_time_original']
        file_type = exif_media_file.file_properties['file_type']

        sql = (
            'SELECT file_id FROM file '
            'WHERE '
            "date_time_original = '{0}' "
            'AND '
            "file_type = '{1}'"
        ).format(date_time_original, file_type)

        db_result = self.execute_sql(sql)
        db_data = db_result.fetchone()
        if db_data is None:
            exif_media_file.file_id = None
            self.logger.debug('no entry in db for capture time: %s', date_time_original)
            return False
        else:
            exif_media_file.file_id = str(db_data[0])
            self.logger.debug('entry for capture time exists db: %s (id: %s)', date_time_original,
                              exif_media_file.file_id)
            return True

    def file_date_type_size_matches(self, exif_media_file: ExifMediaFile):
        """
        Check if there is a match for capture time, file type and file size in db

        :param exif_media_file:
        :return: bool
        """

        assert isinstance(exif_media_file, ExifMediaFile)

        date_time_original = exif_media_file.file_properties['date_time_original']
        file_type = exif_media_file.file_properties['file_type']
        file_size = exif_media_file.file_properties['file_size']

        sql = (
            'SELECT file_id FROM file '
            'WHERE '
            "date_time_original = '{0}' "
            'AND '
            "file_type = '{1}' "
            'AND '
            "file_size = {2}"
        ).format(date_time_original, file_type, file_size)

        db_result = self.execute_sql(sql)
        db_data = db_result.fetchone()
        if db_data is None:
            exif_media_file.file_id = None
            self.logger.debug('no entry in db for capture time, type and size: %s', date_time_original)
            return False
        else:
            # exif_media_file.file_id = str(db_data[0])
            self.logger.debug('entry for capture time, file type and size exists db: %s', date_time_original)
            return True

    def file_size_matches(self, exif_media_file: ExifMediaFile):
        """
        Check if file size match in db

        :param exif_media_file: 
        :return: bool
        """

        assert isinstance(exif_media_file, ExifMediaFile)

        file_size = exif_media_file.file_properties['file_size']

        sql = (
            'SELECT file_id FROM file '
            'WHERE '
            "file_size = {1}"
        ).format(file_size)

        db_result = self.execute_sql(sql)
        db_data = db_result.fetchone()
        if db_data is None:
            self.logger.debug('file size does not match')
            exif_media_file.file_id = None
            return False
        else:
            self.logger.debug('file size matches')
            exif_media_file.file_id = str(db_data[0])
            return True

    def file_type_size_matches(self, exif_media_file: ExifMediaFile):
        """
        Check if there is a match for (file type, file size) in db

        :param exif_media_file:
        :return: bool
        """

        assert isinstance(exif_media_file, ExifMediaFile)

        file_type = exif_media_file.file_properties['file_type']
        file_size = exif_media_file.file_properties['file_size']

        sql = (
            'SELECT file_id FROM file '
            'WHERE '
            "file_type = '{0}' "
            'AND '
            "file_size = {1}"
        ).format(file_type, file_size)

        db_result = self.execute_sql(sql)
        db_data = db_result.fetchone()

        if db_data is None:
            self.logger.debug('file type + size does not match')
            exif_media_file.file_id = None
            return False
        else:
            self.logger.debug('file type + size matches')
            exif_media_file.file_id = str(db_data[0])
            return True

    def file_hash_matches(self, exif_media_file: ExifMediaFile):
        """
        Check if file hash value matches in db

        :param exif_media_file: 
        :return: bool
        """

        assert isinstance(exif_media_file, ExifMediaFile)

        file_hash = exif_media_file.file_properties['file_hash_md5']

        sql = (
            'SELECT file_id FROM file '
            'WHERE '
            "file_hash_md5 = '{0}'"
        ).format(file_hash)

        db_result = self.execute_sql(sql)
        db_data = db_result.fetchone()
        if db_data is None:
            self.logger.debug("file hash value doesn't match")
            exif_media_file.file_id = None
            return False
        else:
            self.logger.debug('file hash value matches')
            exif_media_file.file_id = str(db_data[0])
            return True

    def assign_unique_target_filename(self, exif_media_file: ExifMediaFile):
        """
        Modify target filename to make it unique if it already exists with different content
        :param exif_media_file: 
        """
        base_target_filename = exif_media_file.get_base_target_filename()
        target_file_extension = exif_media_file.file_properties['file_type'].lower()
        target_filename = exif_media_file.file_properties['target_filename']

        self.logger.debug('de-duplicating filename for:' + target_filename)

        counter = 1
        while not self._is_unique_target_filename(target_filename):
            self.logger.info('filename <' + target_filename + '> already exists - adding counter')
            target_filename = base_target_filename + '-' + str(counter) + '.' + target_file_extension
            counter += 1
        else:
            exif_media_file.file_properties['target_filename'] = target_filename
        self.logger.debug('de-duplicated filename:' + target_filename)

    def _is_unique_target_filename(self, target_filename):
        """
        Check if target filename exists in db (i.e. is not unique)
        :param target_filename:
        :return:
        """
        sql = (
            'SELECT file_id FROM file '
            'WHERE '
            "target_filename = '{0}' "
        ).format(target_filename)

        db_result = self.execute_sql(sql)

        if db_result.fetchone() is None:
            return True
        else:
            return False

    def add_file(self, exif_media_file: ExifMediaFile):
        """
        Add new file record for given file
        :param exif_media_file: 
        """
        assert isinstance(exif_media_file, ExifMediaFile)

        target_fields_str = ','.join(exif_media_file.file_properties.keys())
        target_values_str = ','.join("'{0}'".format(v) for v in exif_media_file.file_properties.values())

        sql = 'INSERT INTO file ({0}) values ({1})'.format(target_fields_str, target_values_str)
        db_result = self.execute_sql(sql)

        # store file_id in emf object property
        # TODO: Make this use cur.lastrowid
        sql = "SELECT file_id FROM file WHERE file_hash_md5 = '{0}'".format(
            exif_media_file.file_properties['file_hash_md5']
        )
        db_result = self.execute_sql(sql)
        exif_media_file.file_id = str(db_result.fetchone()[0])

    def update_copy_flags(self, exif_media_file: ExifMediaFile):
        """
        Update fields 'copied' and 'date_copied' for the given file
        
        Note: timestamps are always in GMT! 
        Use "select datetime(date_copied, 'localtime') from file" to get time in current timezone
        
        :param exif_media_file: 
        """
        assert isinstance(exif_media_file, ExifMediaFile)

        if exif_media_file.file_id is not None:
            sql = (
                'UPDATE file '
                'SET copied = 1, date_copied = CURRENT_TIMESTAMP '
                'WHERE file_id = {0}'.format(exif_media_file.file_id)
            )
            self.execute_sql(sql)
        else:
            self.logger.warning("could not update copy flags: target file id is empty in emf!")

    def add_source(self, exif_media_file: ExifMediaFile):
        """
        Record a new source for a given file (the same file may be in different locations (copies))
        :param exif_media_file: 
        """
        assert isinstance(exif_media_file, ExifMediaFile)

        # set file_id property of target in exif_media_file
        if exif_media_file.file_id is not None:
            exif_media_file.source_properties['file_id'] = exif_media_file.file_id

            source_fields_str = ','.join(exif_media_file.source_properties.keys())
            source_values_str = ','.join("'{0}'".format(v) for v in exif_media_file.source_properties.values())

            sql = 'INSERT INTO source ({0}) values ({1})'.format(source_fields_str, source_values_str)
            self.execute_sql(sql)
        else:
            self.logger.warning("could not insert source: target file id is empty in in emf!")

    def fetch_all_records(self):
        """
        print all db records
        """
        sql = (
            "SELECT *, 'db timestamps in GMT!' as 'note' FROM file f "
            'INNER JOIN source s (using file_id) '
            'ORDER by f.date_time_original DESC'
        )

        db_result = self.execute_sql(sql)

        for row in db_result.fetchall():
            print(repr(row))

    def get_target_file_list(self):
        """
        get list of target files in db
        """

        file_list = []

        sql = (
            'SELECT file_id, target_path, target_filename FROM file ORDER by date_added ASC'
        )

        db_result = self.execute_sql(sql)

        if not db_result is None:
            for row in db_result.fetchall():
                row_object = {
                    'file_id': row[0],
                    'relative_path': row[1],
                    'filename': row[2]
                }
                file_list.append(row_object)

        return file_list

    def drop_target_record(self, file_id):
        sql = 'DELETE FROM file WHERE file_id = {0}'.format(file_id)
        self.execute_sql(sql)

    def get_target_path_filename(self, exif_media_file: ExifMediaFile):

        assert isinstance(exif_media_file, ExifMediaFile)

        if not exif_media_file.file_properties['file_hash_md5']:
            exif_media_file.calculate_md5()

        file_md5 = exif_media_file.file_properties['file_hash_md5']

        sql = (
            'SELECT target_path, target_filename FROM file '
            'WHERE '
            "file_hash_md5 = '{0}'"
        ).format(file_md5)

        return self.execute_sql(sql).fetchone()
