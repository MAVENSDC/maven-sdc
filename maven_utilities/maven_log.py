'''
Created on May 26, 2015

@author: cosc3564
@author: bussell
@author: bstaley
'''
import os
from logging import config
from maven_utilities import constants, log_config


def config_logging(cfg_name=None):
    ''' Method used to configure ALL loggers.  This is intended to be called once from __main__
    Arguments:
        cfg_name - The logger configuration name.  This is expected to be a key found in log_config
    '''
    log_cfg = None
    if cfg_name is None:  # Get from Env
        log_cfg_name_env = os.environ.get('LOG_CFG_NAME', None)
        if log_cfg_name_env is None:
            env = os.environ.get(constants.python_env, 'production_readonly')
            if env == 'testing':
                log_cfg = log_config.LOGGING_CFGS['TEST_LOGGING']
            else:
                log_cfg = log_config.LOGGING_CFGS['PROD_LOGGING']
        else:  # LOG_CFG_NAME found in the ENV
            log_cfg = log_config.LOGGING_CFGS[log_cfg_name_env]
    else:  # cfg_name provided in config_logging call
        log_cfg = log_config.LOGGING_CFGS[log_cfg]

    if log_cfg is None:
        print ('Unable to resolve the logging config with:\n\t cfg_name%s\n\t %s %s\n\t LOG_CFG_NAME %s' % (cfg_name, constants.python_env, env, log_cfg_name_env))
        return

    config.dictConfig(log_cfg)
