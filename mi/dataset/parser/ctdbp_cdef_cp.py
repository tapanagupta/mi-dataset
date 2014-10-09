#TBD
#!/usr/bin/env python


__author__ = 'tgupta'
__license__ = 'Apache 2.0'

import copy
import calendar
from functools import partial
import re



from mi.core.instrument.chunker import \
    StringChunker

from mi.core.log import get_logger; log = get_logger()
from mi.core.common import BaseEnum
from mi.core.exceptions import \
    DatasetParserException, \
    SampleException, \
    UnexpectedDataException

from mi.core.instrument.data_particle import \
    DataParticle, \
    DataParticleKey

from mi.dataset.dataset_parser import BufferLoadingParser

# Basic patterns
ANY_CHARS = r'.*'          # Any characters excluding a newline
NEW_LINE = r'(?:\r\n|\n)'  # any type of new line

START_METADATA = r'\*'

HEX_DATA_30 = r'([0-9A-F]{30})'

# Regex for data particle class
DATA_REGEX = r'([0-9A-F]{6})([0-9A-F]{6})([0-9A-F]{6})([0-9A-F]{4})([0-9A-F]{8})'
DATA_MATCHER = re.compile(DATA_REGEX, re.DOTALL)

# All ctdbp records are ASCII characters separated by a newline.
CTDBP_RECORD_PATTERN = ANY_CHARS       # Any number of ASCII characters
CTDBP_RECORD_PATTERN += NEW_LINE       # separated by a new line
CTDBP_RECORD_MATCHER = re.compile(CTDBP_RECORD_PATTERN)

# Metadata record:
METADATA_PATTERN = START_METADATA    # Metadata record starts with '*'
METADATA_PATTERN += ANY_CHARS         # followed by text
METADATA_PATTERN += NEW_LINE         # followed by newline
METADATA_MATCHER = re.compile(METADATA_PATTERN)

# Time tuple corresponding to January 1st, 2000
JAN_1_2000 = (2000, 1, 1, 0, 0, 0, 0, 0, 0)

# Sensor data record:
#  (36 Hex Characters followed by new line)
SENSOR_DATA_PATTERN = HEX_DATA_30 + NEW_LINE
SENSOR_DATA_MATCHER = re.compile(SENSOR_DATA_PATTERN)

class DataParticleType(BaseEnum):
    SAMPLE = 'ctdbp_cdef_cp_instrument_recovered'

class CtdpfParserDataParticleKey(BaseEnum):
    TEMPERATURE = "temperature"
    CONDUCTIVITY = "conductivity"
    PRESSURE = "pressure"
    PRESSURE_TEMP = "pressure_temp"
    CTD_TIME = "ctd_time"

class CtdbpStateKey(BaseEnum):
    POSITION = 'position'            # position within the input file


class CtdbpCdefCpInstrumentDataParticle(DataParticle):
    """
    Class for generating the ctdbp_cdef_cp_instrument_recovered data particle.
    """

    _data_particle_type = DataParticleType.SAMPLE

    def _build_parsed_values(self):
        """
        Take recovered Hex raw data and extract different fields, converting Hex to Integer values.
        @throws SampleException If there is a problem with sample creation
        """

        #log.debug('***************************')
        #log.debug('Raw Data: %s', self.raw_data)

        # the data contains seconds since Jan 1, 2000. Need the number of seconds before that
        SECONDS_TILL_JAN_1_2000 = calendar.timegm(JAN_1_2000)

        match = DATA_MATCHER.match(self.raw_data)
        if not match:
            raise SampleException("CtdbpCdefCpInstrumentDataParticle: No regex match of \
                                  parsed sample data: [%s]", self.raw_data)
        try:
            # grab Hex values, convert to int
            temp = int(match.group(1), 16)
            cond = int(match.group(2), 16)
            press = int(match.group(3), 16)
            press_temp = int(match.group(4), 16)
            ctd_time = int(match.group(5), 16)

            # log.debug('Temp: %s', temp)
            # log.debug('Cond: %s', cond)
            # log.debug('Press: %s', press)
            # log.debug('Press Temp: %s', press_temp)
            # log.debug('Time: %s', ctd_time)

        except (ValueError, TypeError, IndexError) as ex:
            raise SampleException("Error (%s) while decoding parameters in data: [%s]"
                                  % (ex, self.raw_data))

        # calculate the internal timestamp
        elapsed_seconds = SECONDS_TILL_JAN_1_2000 + ctd_time
        self.set_internal_timestamp(unix_time=elapsed_seconds)

        result = [{DataParticleKey.VALUE_ID: CtdpfParserDataParticleKey.TEMPERATURE,
                   DataParticleKey.VALUE: temp},
                  {DataParticleKey.VALUE_ID: CtdpfParserDataParticleKey.CONDUCTIVITY,
                   DataParticleKey.VALUE: cond},
                  {DataParticleKey.VALUE_ID: CtdpfParserDataParticleKey.PRESSURE,
                   DataParticleKey.VALUE: press},
                  {DataParticleKey.VALUE_ID: CtdpfParserDataParticleKey.PRESSURE_TEMP,
                   DataParticleKey.VALUE: press_temp},
                  {DataParticleKey.VALUE_ID: CtdpfParserDataParticleKey.CTD_TIME,
                   DataParticleKey.VALUE: ctd_time}]
        log.debug('CtdbpCdefCpInstrumentDataParticle: particle=%s', result)

        #log.debug('***************************')

        return result


class CtdbpCdefCpParser(BufferLoadingParser):

    """
    Parser for CTDBP_cdef_cp data.
    In addition to the standard constructor parameters,
    this constructor takes an additional parameter particle_class.
    """
    def __init__(self,
                 config,
                 stream_handle,
                 state,
                 state_callback,
                 publish_callback,
                 exception_callback,
                 particle_class,
                 *args, **kwargs):

        # No fancy sieve function needed for this parser.
        # File is ASCII with records separated by newlines.

        super(CtdbpCdefCpParser, self).__init__(config,
                                          stream_handle,
                                          state,
                                          partial(StringChunker.regex_sieve_function,
                                                  regex_list=[CTDBP_RECORD_MATCHER]),
                                          state_callback,
                                          publish_callback,
                                          exception_callback,
                                          *args,
                                          **kwargs)

        # Default the position within the file to the beginning.

        self._read_state = {CtdbpStateKey.POSITION: 0}
        self.input_file = stream_handle
        self.particle_class = particle_class

        # If there's an existing state, update to it.

        if state is not None:
            self.set_state(state)

    def handle_non_data(self, non_data, non_end, start):
        """
        Handle any non-data that is found in the file
        """
        # Handle non-data here.
        # Increment the position within the file.
        # Use the _exception_callback.
        if non_data is not None and non_end <= start:
            self._increment_position(len(non_data))
            self._exception_callback(UnexpectedDataException(
                "Found %d bytes of un-expected non-data %s" %
                (len(non_data), non_data)))

    def _increment_position(self, bytes_read):
        """
        Increment the position within the file.
        @param bytes_read The number of bytes just read
        """
        self._read_state[CtdbpStateKey.POSITION] += bytes_read

    def parse_chunks(self):
        """
        Parse out any pending data chunks in the chunker.
        If it is valid data, build a particle.
        Go until the chunker has no more valid data.
        @retval a list of tuples with sample particles encountered in this
            parsing, plus the state.
        """
        result_particles = []
        (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
        (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index(clean=True)
        self.handle_non_data(non_data, non_end, start)

        while chunk is not None:

            log.debug("CHUNK: %s", chunk)
            self._increment_position(len(chunk))

            # If this is a valid sensor data record,
            # use the extracted fields to generate a particle.

            sensor_match = SENSOR_DATA_MATCHER.match(chunk)

            if sensor_match is not None:

                log.debug("Sensor match found!")

                particle = self._extract_sample(self.particle_class,
                                                None,
                                                chunk,
                                                None)

                log.debug("GOT PARTICLE!!!")

                if particle is not None:
                    result_particles.append((particle, copy.copy(self._read_state)))

            # It's not a sensor data record, see if it's a metadata record.

            else:

                # If it's a valid metadata record, ignore it.
                # Otherwise generate warning for unknown data.

                meta_match = METADATA_MATCHER.match(chunk)

                log.debug("Meta match: %s", str(meta_match))
                if meta_match is None:
                    error_message = 'Unknown data found in chunk %s' % chunk
                    log.warn(error_message)
                    self._exception_callback(UnexpectedDataException(error_message))

            (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
            (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index(clean=True)
            self.handle_non_data(non_data, non_end, start)

        return result_particles

    def set_state(self, state_obj):
        """
        Set the value of the state object for this parser
        @param state_obj The object to set the state to.
        @throws DatasetParserException if there is a bad state structure
        """
        if not isinstance(state_obj, dict):
            raise DatasetParserException("Invalid state structure")

        if not (CtdbpStateKey.POSITION in state_obj):
            raise DatasetParserException('%s missing in state keys' %
                                         CtdbpStateKey.POSITION)

        self._record_buffer = []
        self._state = state_obj
        self._read_state = state_obj

        self.input_file.seek(state_obj[CtdbpStateKey.POSITION])