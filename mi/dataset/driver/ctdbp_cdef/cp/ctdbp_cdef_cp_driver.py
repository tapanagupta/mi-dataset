from mi.dataset.parser.ctdbp_cdef_cp import CtdbpCdefCpParser
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

def state_callback(state,file):
    pass
def pub_callback(state):
    pass
def exception_callback(state):
    pass

def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    with open(sourceFilePath,"r") as fil :
        parser = CtdbpCdefCpParser(
            {DataSetDriverConfigKeys.PARTICLE_MODULE:"mi.dataset.parser.ctdbp_cdef_cp",DataSetDriverConfigKeys.PARTICLE_CLASS:None},
            fil,
            None,
            state_callback,
            pub_callback,
            exception_callback)
        driver = DataSetDriver(parser, particleDataHdlrObj)
        driver.processFileStream()
    return particleDataHdlrObj
