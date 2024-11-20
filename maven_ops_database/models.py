'''
Created on Nov 5, 2015

@author: bstaley
'''

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, CHAR, CLOB
from sqlalchemy.orm import relationship
from maven_ops_database.database import Base


class OpsMissionEventType(Base):

    __tablename__ = "MISSIONEVENTTYPES"

    eventtypeid = Column(Integer, primary_key=True)
    name = Column(String)
    label = Column(String)
    isdiscrete = Column(CHAR)
    description = Column(String)
    discussion = Column(CLOB)

    maven_events = relationship("OpsMissionEvent", backref="ops_event_type")

    def __init__(self,
                 eventtypeid,
                 name,
                 label,
                 isdiscrete,
                 description,
                 discussion):

        self.eventtypeid = eventtypeid
        self.name = name
        self.label = label
        self.is_discrete = isdiscrete
        self.description = description
        self.discussion = discussion

    def __str__(self):
        return '%d %s %s' % (self.eventtypeid, self.name, self.label)

    __repr__ = __str__


class OpsMissionEvent(Base):

    __tablename__ = "MISSIONEVENTS"

    eventid = Column(Integer, primary_key=True)
    eventtypeid = Column(Integer, ForeignKey("MISSIONEVENTTYPES.eventtypeid"), nullable=False)
    starttime = Column(DateTime(timezone=True), nullable=False)
    endtime = Column(DateTime(timezone=True), nullable=False)
    source = Column(String, nullable=False)
    description = Column(String, nullable=True)
    discussion = Column(CLOB, nullable=True)

    orbit_numbers = relationship("OpsMissionEventOrbitNumber", backref="maven_event")

    def __init__(self,
                 eventid,
                 eventtypeid,
                 starttime,
                 endtime,
                 source,
                 description,
                 discussion):

        self.eventid = eventid
        self.eventtypeid = eventtypeid
        self.starttime = starttime
        self.endtime = endtime
        self.source = source
        self.description = description
        self.discussion = discussion

    def __str__(self):
        return '%s %s %s %s' % (self.eventid, self.eventtypeid, self.starttime, self.endtime)

    __repr__ = __str__


class OpsMissionEventOrbitNumber(Base):

    __tablename__ = "ORBITNUMBER"

    eventid = Column(Integer, ForeignKey("MISSIONEVENTS.eventid"), primary_key=True, nullable=False)
    orbitnumber = Column(Integer, nullable=False)

    def __init__(self,
                 event_id,
                 orbit_number):

        self.eventid = event_id
        self.orbitnumber = orbit_number

    def __str__(self):
        return '%d %d' % (self.eventid, self.orbitnumber)

    __repr__ = __str__
