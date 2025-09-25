import os
import logging
from maven_utilities import constants

log_name = os.environ.get('LOG_NAME', 'maven_log')
log_path = '/maven/mavenpro/logs'

INSERT_SCIENCE_EVENT_LOGGING_CFG = 'INSERT_SCIENCE_EVENT_LOGGING'

PROD_LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    # THIS OVERWRITES EXISTING LOGGER CONFIGURATIONS. SEE DOCS
    'incremental': False,
    'formatters': {
        'format1': {
            'format': '%(asctime)s - %(name)s: %(levelname)s L%(lineno)d %(message)s'
        },
        'format_TID': {
            'format': '%(asctime)s - %(thread)s - %(name)s: %(levelname)s L%(lineno)d %(message)s'
        },
        'db_instance_format': {
            'format': '%(asctime)s - %(name)s: %(process)d  %(levelname)s L%(lineno)d %(message)s'
        },
    },
    'filters': {},
    'handlers': {
        'console1': {
            'level': logging.DEBUG,
            'class': 'logging.StreamHandler',
            'formatter': 'format1'
        },
        'console_TID': {
            'level': logging.DEBUG,
            'class': 'logging.StreamHandler',
            'formatter': 'format_TID'
        },
        'null': {
            'class': 'logging.NullHandler'
        },
        'db_log': {
            'level': logging.DEBUG,
            '()': 'maven_database.maven_db_log_handler.MavenDbLogHandler',
            'formatter': 'db_instance_format'
        },
        'fileFormat_TID': {
            'level': logging.DEBUG,
            'class': 'logging.FileHandler',
            'filename': os.path.join(log_path, log_name),
            'formatter': 'format_TID'
        },
        'fileRotate': {
            'level': logging.DEBUG,
            'class': 'logging.handlers.RotatingFileHandler',
            'maxBytes': constants.log_maxByte_size,
            'backupCount': constants.log_backup_count,
            'filename': os.path.join(log_path, log_name),
            'formatter': 'format1'
        }
    },
    'loggers': {
        'maven.make_pds_bundles': {
            'handlers': ['console1'],
            'level': logging.DEBUG
        },
        'maven.maven_file_cleaner.utilities.log': {
            'handlers': ['console1'],
            'level': logging.DEBUG
        },
        'maven.make_pds_bundles.make_pds_bundles.dryrunlog': {
        },
        'maven.make_pds_bundles.make_pds_bundles.directlog': {
        },
        'maven.make_pds_bundles.make_pds_bundles.log': {
            'handlers': ['fileRotate']
        },
        'maven.maven_data_availability': {
            'handlers': ['fileRotate', 'console1'],
            'level': logging.DEBUG
        },
        'maven.maven_data_availability.utilities': {
            'handlers': ['fileRotate', 'console1'],
            'level': logging.DEBUG
        },
        'maven.maven_data_file_indexer': {
            'handlers': ['fileFormat_TID'],
            'level': logging.INFO
        },
        'maven.maven_data_file_indexer.index_worker.log': {
            'handlers': ['fileFormat_TID'],
            'level': logging.DEBUG
        },
        'maven.maven_data_file_indexer.maven_data_file_indexer.log': {
            'level': logging.DEBUG 
        },
        'maven.maven_events_api': {
            'handlers': ['fileRotate', 'console1'],
            'level': logging.DEBUG
        },
        'maven.maven_science_files_api': {
            'handlers': ['fileRotate', 'console1'],
            'level': logging.DEBUG
        },
        'maven.maven_dropbox_mgr': {
            'handlers': ['fileRotate'],
            'level': logging.DEBUG
        },

        'maven.maven_dropbox_mgr.config.log': {
        },
        'maven.maven_dropbox_mgr.utilities.log': {
            'handlers': ['fileFormat_TID'],
            'level': logging.DEBUG,  # used by the ELK stack to track misnamed files
            'propagate': False
        },
        'maven.maven_dropbox_mgr.utilities.db_log': {
            'handlers': ['db_log'],
            'propagate': False,
            'level': logging.DEBUG
        },
        'maven.maven_status.status.log': {  # careful setting this to console..  CronJob will resend stdout (causing an infinite loop_
            'handlers': ['fileRotate'],
            'level': logging.DEBUG
        },
        'maven.maven_tools.es_auditor': {
            'handlers': ['fileRotate'],
            'level': logging.DEBUG
        },
        'maven.update_events.log': {
            'handlers': ['fileRotate'],
            'level': logging.INFO,
            'propagate': False
        },
        'maven.update_events.dblog': {
            'handlers': ['db_log'],
            'level': logging.INFO,
            'propagate': False
        },
        'maven.ingest_anc_files': {
            'handlers': ['fileFormat_TID'],
            'level': logging.DEBUG  # used by the ELK stack to track misnamed files
        },
        'maven.ingest_anc_files.utilities.db_log': {
            'handlers': ['db_log'],
            'propagate': False,
            'level': logging.DEBUG
        },
        'maven.ingest_anc_files.utilities.log': {
        },
        'maven.ingest_anc_files.build_trk_bundle.log': {
            'handlers': ['fileRotate'],
            'level': logging.INFO,
            'propagate': False
        },
        'maven.ingest_l0_files': {
            'handlers': ['fileFormat_TID'],
            'level': logging.DEBUG  # used by the ELK stack to track misnamed files
        },
        'maven.ingest_l0_files.utilities.log': {
        },
        'maven.ingest_l0_files.utilities.db_log': {
            'handlers': ['db_log'],
            'propagate': False,
            'level': logging.DEBUG
        },
        'maven.ingest_spice_kernels': {
            'handlers': ['null'],
            'level': logging.DEBUG
        },
        'maven.ingest_spice_kernels.utilities.log': {
        },
        'maven.ingest_spice_kernels.utilities.db_log': {
            'handlers': ['db_log']
        },
        'maven.maven_in_situ_kp_file_ingester.utilities.db_log': {
            'handlers': ['db_log']
        },
        'maven.maven_in_situ_kp_file_ingester.maven_in_situ_kp_file_processor.log': {
            'handlers': ['fileRotate', 'console1'],
            'level': logging.INFO
        },
        'maven.maven_utilities.progress.log': {
            'handlers': ['fileRotate', 'console1'],
            'level': logging.DEBUG
        },
        'maven.maven_utilities.file_pattern.log': {
            'handlers': ['fileRotate', 'console1'],
            'level': logging.DEBUG
        },
        'maven.monitor_sdc_web_services.log': {
            'handlers': ['fileRotate'],
            'level': logging.INFO
        },
        'maven.monitor_sdc_web_services.console': {
            'handlers': ['console1'],
            'level': logging.INFO
        },
        'maven.maven_events_api.insert_science_event.log': {
            'handlers': ['fileRotate', 'console1'],
            'level': logging.DEBUG,
            'propagate': False
        },
        'maven.maven_public.utilities.log': {
            'handlers': ['fileRotate', 'console1'],
            'level': logging.DEBUG
        },
    }
}

TEST_LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    # THIS OVERWRITES EXISTING LOGGER CONFIGURATIONS. SEE DOCS
    'incremental': False,
    'formatters': {
        'format1': {
            'format': '%(asctime)s - %(name)s: %(levelname)s L%(lineno)d %(message)s'
        },
    },
    'filters': {},
    'handlers': {
        'console': {
            'level': logging.DEBUG,
            'class': 'logging.StreamHandler',
            'formatter': 'format1'
        },
        'db_log': {
            'level': logging.DEBUG,
            '()': 'maven_database.maven_db_log_handler.MavenDbLogHandler',
            'formatter': 'format1'
        }
    },
    'loggers': {
        'root': {
            'handlers': ['console'],
            'level': logging.DEBUG,
        },
        'maven.maven_file_cleaner.utilities.log': {
            'handlers': ['console'],
            'level': logging.DEBUG
        },
        'maven.maven_dropbox_mgr.utilities.db_log': {
            'handlers': ['db_log'],
            'propagate': False,
            'level': logging.DEBUG,
        },
        'maven.ingest_iuvs_kp_file.utilities.db_log': {
            'handlers': ['db_log'],
            'level': logging.DEBUG,
            'propagate': False
        },
        'maven.maven_in_situ_kp_file_ingester.utilities.db_log': {
            'handlers': ['db_log'],
            'level': logging.INFO,
            'propagate': False
        },
        'maven.maven_in_situ_kp_file_ingester.maven_in_situ_kp_file_processor.log': {
            'handlers': ['console'],
            'level': logging.INFO,
            'propagate': False
        },
        'maven.maven_science_files_api.maven_science_files_api.utilities.log': {
            'handlers': ['console'],
            'level': logging.DEBUG
        },
        'maven.update_events.log': {
            'handlers': ['console'],
            'level': logging.INFO,
            'propagate': False
        },
        'maven.update_events.dblog': {
            'handlers': ['db_log'],
            'level': logging.INFO,
            'propagate': False
        },
        'maven.maven_database.tests.db_log': {
            'handlers': ['db_log'],
            'level': logging.DEBUG,
            'propagate': False
        },
        'maven.maven_data_file_indexer.index_worker.log': {
            'handlers': ['console'],
            'level': logging.DEBUG
        },
        'maven.maven_data_file_indexer.maven_data_file_indexer.log': {
            'level': logging.DEBUG 
        },
        'maven.maven_utilities.file_pattern.log': {
            'handlers': ['console'],
            'level': logging.DEBUG
        },
        'maven.monitor_sdc_web_services.log': {
            'handlers': ['console'],
            'level': logging.INFO
        },
        'maven.make_pds_bundles': {
            'handlers': ['console'],
            'level': logging.DEBUG
        },
        'maven.make_pds_bundles.make_pds_bundles.dryrunlog': {
        },
        'maven.make_pds_bundles.make_pds_bundles.directlog': {
            'level': logging.DEBUG
        },
        'maven.maven_public.utilities.log': {
            'handlers': ['console'],
            'level': logging.DEBUG

        },
    }
}

LOGGING_CFGS = {
    'PROD_LOGGING': PROD_LOGGING,
    'TEST_LOGGING': TEST_LOGGING
}
