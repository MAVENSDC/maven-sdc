'''Database models for the sqlite database used for storing archive information.

@author: Mike Dorey
'''
from . import config
from peewee import Model, SqliteDatabase
from peewee import TextField, ForeignKeyField, DateTimeField, IntegerField


db = SqliteDatabase(config.db_uri)


class BaseModel(Model):
    '''Base of the Glacier model classes.'''

    class Meta:
        database = db


class GlacierVault(BaseModel):
    '''Amazon Glacier vaults'''

    class Meta:
        db_table = "glacier_vaults"
    
    aws_region = TextField()
    vault_name = TextField()

    def __str__(self):
        '''Returns a string representation of this object.'''
        return '%s %s' % (self.aws_region, self.vault_name)

    __repr__ = __str__


class GlacierArchive(BaseModel):
    '''Amazon Glacier archives'''

    class Meta:
        db_table = "glacier_archives"

    glacier_vault = ForeignKeyField(GlacierVault, related_name='archives')
    glacier_id = TextField()
    glacier_description = TextField()
    md5_hash = TextField()
    when_archived = DateTimeField()

    def __str__(self):
        '''Returns a string representation of this object.'''
        return '%s %s' % (self.glacier_description, self.when_archived)

    __repr__ = __str__


class GlacierArchiveFile(BaseModel):
    '''File in an Amazon Glacier archive'''

    class Meta:
        db_table = "glacier_archive_files"

    glacier_archive = ForeignKeyField(GlacierArchive, related_name='files')
    file_path = TextField()
    file_size = IntegerField()
    md5_hash = TextField()
    modification_time = DateTimeField()

    def __str__(self):
        '''Returns a string representation of this object.'''
        return '%s %d %s' % (self.file_path, self.file_size, self.modification_time)

    __repr__ = __str__


class ErrorLog(BaseModel):
    '''Error log.'''

    class Meta:
        db_table = "error_logs"

    error_message = TextField()
    when_logged = DateTimeField()

    def __str__(self):
        '''Returns a string representation of the message.'''
        return "%s: %s" % (self.when_logged, self.error_message)

    __repr__ = __str__
