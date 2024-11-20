from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from maven_database.database import engine
from maven_database.models import MavenOrbit

orbit_db_session = scoped_session(sessionmaker(autocommit=False,
                                               autoflush=False,
                                               bind=engine,
                                               expire_on_commit=False))

# Just in case we want to use ORB access via MavenOrbit
MavenOrbit.query = orbit_db_session.query_property()
