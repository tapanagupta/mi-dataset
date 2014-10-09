import unittest
import os
from nose.plugins.attrib import attr

from mi.core.log import get_logger; log = get_logger()

from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.ctdbp_cdef_cp import CtdbpCdefCpParser
from mi.dataset.parser.ctdbp_cdef_cp import CtdbpCdefCpInstrumentDataParticle

from mi.dataset.parser.ctdbp_cdef_cp import  DATA_MATCHER
from mi.dataset.parser.ctdbp_cdef_cp import  CtdbpStateKey

from mi.idk.config import Config

from mi.core.exceptions import SampleException

RESOURCE_PATH = os.path.join(Config().base_dir(), 'mi', 'dataset', 'driver',
                             'ctdbp_cdef', 'cp', 'resource')

MODULE_NAME = 'mi.dataset.parser.ctdbp_cdef_cp'

RAW_INPUT_DATA_1 = "raw_input1.log"
EXTRACTED_DATA_FILE = "extracted_data.log"
INVALID_DATA_FILE = "invalid_data.log"
NO_SENSOR_DATA_FILE = "no_sensor_data.log"
RECORDS_FILE_30  = "data2.log"
RECORDS_FILE_SET_STATE  = "data3.log"
DATA_FILE_1 = "data1.log"

# Define number of expected records/exceptions for various tests
NUM_REC_DATA_FILE1 = 100
NUM_REC_MID_START = 15
NUM_REC_SET_STATE = 10
NUM_INVALID_EXCEPTIONS = 7

YAML_FILE = "data1.yml"
YAML_FILE_MID_START = "data_mid.yml"
YAML_FILE_SET_STATE1= "data_set_state1.yml"
YAML_FILE_SET_STATE2= "data_set_state2.yml"
YAML_FILE_SET_STATE3= "data_set_state3.yml"


@attr('UNIT', group='mi')
class CtdbpCdefCpParserUnitTestCase(ParserUnitTestCase):
    """
    ctdbp_cdef_ce Parser unit test suite
    """

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
        This function creates a CtdbpCdefCp parser for recovered data.
        """
        parser = CtdbpCdefCpParser(self.rec_config,
            file_handle, new_state, self.rec_state_callback,
            self.rec_pub_callback, self.rec_exception_callback, CtdbpCdefCpInstrumentDataParticle)
        return parser


    def test_verify_record_against_yaml(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== START YAML TEST =====')
        in_file = self.open_file(DATA_FILE_1)
        parser = self.create_rec_parser(in_file)

        # In a single read, get all particles in this file.
        number_expected_results = NUM_REC_DATA_FILE1
        result = parser.get_records(number_expected_results)
        self.assert_particles(result, YAML_FILE, RESOURCE_PATH)

        in_file.close()
        self.assertEqual(self.rec_exception_callback_value, None)

        log.debug('===== END YAML TEST =====')


    def test_invalid_sensor_data_records(self):
        """
        Read data from a file containing invalid sensor data records.
        Verify that no instrument particles are produced
        and the correct number of exceptions are detected.
        """
        log.debug('===== START TEST INVALID SENSOR DATA =====')
        in_file = self.open_file(INVALID_DATA_FILE)
        parser = self.create_rec_parser(in_file)

        # Try to get records and verify that none are returned.
        result = parser.get_records(1)
        self.assertEqual(result, [])
        self.assertEqual(self.rec_exceptions_detected, NUM_INVALID_EXCEPTIONS)

        in_file.close()

        log.debug('===== END TEST INVALID SENSOR DATA =====')


    def test_no_sensor_data(self):
        """
        Read a file containing no sensor data records
        and verify that no particles are produced.
        """
        log.debug('===== START TEST NO SENSOR DATA RECOVERED =====')
        in_file = self.open_file(NO_SENSOR_DATA_FILE)
        parser = self.create_rec_parser(in_file)

        # Try to get a record and verify that none are produced.
        result = parser.get_records(1)
        self.assertEqual(result, [])

        self.assertEqual(self.rec_exception_callback_value, None)
        in_file.close()

        log.debug('===== END TEST NO SENSOR DATA =====')


    def test_mid_state_start(self):
        """
        Test starting a parser with a state in the middle of processing.
        """
        log.debug('===== START TEST MID-STATE START RECOVERED =====')

        in_file = self.open_file(RECORDS_FILE_30)

        # Start at the beginning of record 15 (of 24 total)
        initial_state = {
            CtdbpStateKey.POSITION: 548
        }

        parser = self.create_rec_parser(in_file, new_state=initial_state)

        # In a single read, get all particles in this file.
        number_expected_results = NUM_REC_MID_START
        result = parser.get_records(number_expected_results)
        self.assert_particles(result, YAML_FILE_MID_START, RESOURCE_PATH)

        self.assertEqual(self.rec_exception_callback_value, None)
        in_file.close()

        log.debug('===== END TEST MID-STATE START RECOVERED =====')


    def test_set_state(self):
        """
        This test verifies that the state can be changed after starting.
        Some particles are read and then the parser state is modified to
        skip ahead or back.
        """
        log.debug('===== START TEST SET STATE RECOVERED =====')

        in_file = self.open_file(RECORDS_FILE_SET_STATE)
        parser = self.create_rec_parser(in_file)

        # Get the first 10 particles in this file
        number_expected_results = NUM_REC_SET_STATE
        result = parser.get_records(number_expected_results)
        self.assert_particles(result, YAML_FILE_SET_STATE1, RESOURCE_PATH)

        # Skip ahead in the file so that we get the last 10 particles.
        new_state = {
            CtdbpStateKey.POSITION: 620
        }

        # Set the state.
        parser.set_state(new_state)

        # Read and verify the last 10 particles.
        number_expected_results = NUM_REC_SET_STATE
        result = parser.get_records(number_expected_results)
        self.assert_particles(result, YAML_FILE_SET_STATE2, RESOURCE_PATH)

        # Skip back in the file so that we get 10 particles prior to the last 10.
        new_state = {
            CtdbpStateKey.POSITION: 310
        }

        # Set the state.
        parser.set_state(new_state)

        # Read and verify 10 particles.
        number_expected_results = NUM_REC_SET_STATE
        result = parser.get_records(number_expected_results)
        self.assert_particles(result, YAML_FILE_SET_STATE3, RESOURCE_PATH)

        self.assertEqual(self.rec_exception_callback_value, None)
        in_file.close()

        log.debug('===== END TEST SET STATE RECOVERED =====')


    # This is not really a test. This is a little module to read in a log file, and extract fields from the data,
    # converting Hex values to Integers. It writes the converted data to a log file, in a format that can be easily
    # imported into a spreadsheet where the data is then converted to yaml format.
    # The reason for including this here is that data validation is also being performed.
    def extract_data_particle_from_log_file(self):
        in_file = self.open_file(RAW_INPUT_DATA_1)
        out_file = self.open_file_write(EXTRACTED_DATA_FILE)

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
                ctd_time = str(int(match.group(5), 16))

                outline = temp + '\t' + cond + '\t' + press + '\t' + press_temp + '\t' + ctd_time + '\n'

                out_file.write(outline)

            except (ValueError, TypeError, IndexError) as ex:
                raise SampleException("Error (%s) while decoding parameters in data: [%s]"
                                  % (ex, line))

        in_file.close()
        out_file.close()

        self.assertEqual(self.rec_exception_callback_value, None)
