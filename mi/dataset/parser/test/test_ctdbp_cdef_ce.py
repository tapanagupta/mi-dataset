import unittest
import os
from nose.plugins.attrib import attr

from mi.core.log import get_logger; log = get_logger()

from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.ctdbp_cdef_ce import CtdbpCdefCeParser
from mi.dataset.parser.ctdbp_cdef_ce import CtdbpCdefCeInstrumentDataParticle

from mi.dataset.parser.ctdbp_cdef_ce import  DATA_MATCHER
from mi.idk.config import Config

from mi.core.exceptions import SampleException

RESOURCE_PATH = os.path.join(Config().base_dir(), 'mi', 'dataset', 'driver',
                             'ctdbp_cdef', 'ce', 'resource')

MODULE_NAME = 'mi.dataset.parser.ctdbp_cdef_ce'

SIMPLE_LOG_FILE = "simple_test.log"
SIMPLE_LOG_FILE_META = "simple_test_meta.log"

RAW_INPUT_DATA_1 = "raw_input1.log"

OUTPUT_HEX_FILE = "hex_data.log"

NUM_REC_SIMPLE_LOG_FILE = 5


@attr('UNIT', group='mi')
class CtdbpCdefCeParserUnitTestCase(ParserUnitTestCase):

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.rec_config = {
            DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
            DataSetDriverConfigKeys.PARTICLE_CLASS: None
        }

        self.rec_state_callback_value = None
        self.rec_file_ingested_value = False
        self.rec_publish_callback_value = None
        self.rec_exception_callback_value = None
        self.rec_exceptions_detected = 0

        self.maxDiff = None

    def open_file(self, filename):
        file = open(os.path.join(RESOURCE_PATH, filename), mode='r')
        return file

    def open_file_write(self, filename):
        file = open(os.path.join(RESOURCE_PATH, filename), mode='w')
        return file

    def rec_state_callback(self, state, file_ingested):
        """ Call back method to watch what comes in via the position callback """
        self.rec_state_callback_value = state
        self.rec_file_ingested_value = file_ingested

    def rec_pub_callback(self, pub):
        """ Call back method to watch what comes in via the publish callback """
        self.rec_publish_callback_value = pub

    def rec_exception_callback(self, exception):
        """ Call back method to watch what comes in via the exception callback """
        self.rec_exception_callback_value = exception
        self.rec_exceptions_detected += 1

    def create_rec_parser(self, file_handle, new_state=None):
        """
        This function creates a CtdbpCdefCe parser for recovered data.
        """
        parser = CtdbpCdefCeParser(self.rec_config,
            file_handle, new_state, self.rec_state_callback,
            self.rec_pub_callback, self.rec_exception_callback, CtdbpCdefCeInstrumentDataParticle)
        return parser

    def test_verify_record(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== START SIMPLE TEST =====')
        in_file = self.open_file(SIMPLE_LOG_FILE)
        parser = self.create_rec_parser(in_file)

        # In a single read, get all particles in this file.
        number_expected_results = NUM_REC_SIMPLE_LOG_FILE
        result = parser.get_records(number_expected_results)
        self.assertEqual(len(result), number_expected_results)

        in_file.close()
        self.assertEqual(self.rec_exception_callback_value, None)

        log.debug('===== END SIMPLE TEST =====')

    def test_verify_record_with_metadata(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== START SIMPLE TEST =====')
        in_file = self.open_file(SIMPLE_LOG_FILE_META)
        parser = self.create_rec_parser(in_file)

        # In a single read, get all particles in this file.
        number_expected_results = NUM_REC_SIMPLE_LOG_FILE
        result = parser.get_records(number_expected_results)
        self.assertEqual(len(result), number_expected_results)

        in_file.close()
        self.assertEqual(self.rec_exception_callback_value, None)

        log.debug('===== END SIMPLE TEST =====')

    def create_hex_data_from_log_file(self):
        in_file = self.open_file(RAW_INPUT_DATA_1)
        out_file = self.open_file_write(OUTPUT_HEX_FILE)

        for line in in_file:

            match = DATA_MATCHER.match(line)
            if not match:
                raise SampleException("CtdParserDataParticle: No regex match of \
                                  parsed sample data: [%s]", line)
            try:
                # grab Hex values, convert to int
                temp = str(int(match.group(1), 16))
                cond = str(int(match.group(2), 16))
                press = str(int(match.group(3), 16))
                press_temp = str(int(match.group(4), 16))
                o2 = str(int(match.group(5), 16))
                ctd_time = str(int(match.group(6), 16))

                outline = temp + ' ' + cond + ' ' + press + ' ' + press_temp + ' ' + o2 + ' ' + ctd_time + '\n'

                out_file.write(outline)

            except (ValueError, TypeError, IndexError) as ex:
                raise SampleException("Error (%s) while decoding parameters in data: [%s]"
                                  % (ex, line))

        in_file.close()
        out_file.close()

        self.assertEqual(self.rec_exception_callback_value, None)