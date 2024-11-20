import os

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from maven_database import config
from maven_utilities import constants

engine = create_engine(config.db_uri)

db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))


class TableNameMixin():

    @declared_attr
    def __tablename__(self):
        return self.__base_tablename__ + os.environ.get(constants.MAVEN_DB_TABLE_SUFFIX, '')


Base = declarative_base()
Base.query = db_session.query_property()
