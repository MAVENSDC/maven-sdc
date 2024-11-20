from distutils.core import setup
import os

version = os.environ["MAVEN_VERSION"]

setup(name='maven_monitor_sdc_web_services',
      version=version,
      description='''Periodic tests to monitor the MAVEN web-services''',
      author="Cora",
      author_email='mavensdc@lasp.colorado.edu',
      scripts=['monitor_sdc_web_services.py', 'monitor-sdc-web-services.sh']
      )
