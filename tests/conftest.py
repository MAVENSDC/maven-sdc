import os

# Set the environment variables so that database initializes with sqlite
from maven_utilities import constants
os.environ[constants.python_env] = 'testing'
os.environ['PYTHON_DB'] = 'sqlite:///:memory:'
os.environ['OPS_DB'] = 'sqlite:///:memory:'

# Create the sqlite database tables
from maven_database import database
from maven_database.models import *
database.Base.metadata.create_all(bind=database.engine)