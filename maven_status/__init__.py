from enum import Enum
global_component_id = []


class MAVEN_SDC_EVENTS(Enum):
    START = 1
    SUCCESS = 2
    PROGRESS = 3
    FAIL = 4
    STATUS = 5
    CANCELLED = 6
    OUTPUT = 7


class MAVEN_SDC_COMPONENT(Enum):
    DROPBOX = 1
    FULL_INDEXER = 2
    DELTA_INDEXER = 3
    PDS_ARCHIVER = 4
    KP_INGESTER = 5
    ANC_INGESTER = 6
    L0_INGESTER = 7
    SPICE_INGESTER = 8
    NEW_MONITOR = 9
    QL_MONITOR = 10
    AUDITOR = 11
    ES_AUDITOR = 12
    ORBIT = 13
    WEB_MONITOR = 14
    DISK_CLEANER = 15
