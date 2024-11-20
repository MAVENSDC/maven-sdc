import os
from sqlalchemy import create_engine

from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from . import config
from maven_utilities import constants


env = os.environ.get(constants.python_env)

# set echo=True to log all sql statements to console
if env == 'testing':
    engine = create_engine(config.ops_db_uri,
                           #convert_unicode=True,
                           echo=False,
                           echo_pool=False,
                           pool_size=1)
else:
    engine = create_engine(config.ops_db_uri,
                           #convert_unicode=True,
                           echo=False,
                           echo_pool=False,
                           pool_size=1,
                           pool_recycle=15 * 60,  # 15 mins
                           max_overflow=1)

db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))
Base = declarative_base()
Base.query = db_session.query_property()

# create in-memory DB


def init_db():
    if env == 'testing' and config.ops_db_uri == 'sqlite:///:memory:':
        Base.metadata.create_all(bind=engine)
