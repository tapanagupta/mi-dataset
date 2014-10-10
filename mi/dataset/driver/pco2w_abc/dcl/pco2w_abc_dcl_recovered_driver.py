##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

__author__ = "jpadula"

import os

from mi.logging import config

from mi.dataset.dataset_parser import DataSetDriverConfigKeys

from mi.dataset.driver.pco2w_abc.dcl.driver_common import Pco2wAbcDclDriver
from mi.dataset.parser.pco2w_abc import Pco2wAbcParticleClassKey
from mi.dataset.parser.pco2w_abc_particles import Pco2wAbcDclMetadataRecoveredDataParticle, \
    Pco2wAbcDclPowerRecoveredDataParticle, \
    Pco2wAbcDclInstrumentRecoveredDataParticle, \
    Pco2wAbcDclInstrumentBlankRecoveredDataParticle


def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):

    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))

    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.pco2w_abc_particles',
        DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
            Pco2wAbcParticleClassKey.METADATA_PARTICLE_CLASS: Pco2wAbcDclMetadataRecoveredDataParticle,
            Pco2wAbcParticleClassKey.POWER_PARTICLE_CLASS: Pco2wAbcDclPowerRecoveredDataParticle,
            Pco2wAbcParticleClassKey.INSTRUMENT_PARTICLE_CLASS: Pco2wAbcDclInstrumentRecoveredDataParticle,
            Pco2wAbcParticleClassKey.INSTRUMENT_BLANK_PARTICLE_CLASS:
            Pco2wAbcDclInstrumentBlankRecoveredDataParticle,
        }
    }

    driver = Pco2wAbcDclDriver(sourceFilePath, particleDataHdlrObj, parser_config)

    return driver.process()
