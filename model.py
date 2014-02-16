# -*- coding: utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy import Column, Table, ForeignKey, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref

engine = create_engine('mysql+mysqldb://root:123123_@127.0.0.1:3306/detenidos_db?charset=utf8')
Base = declarative_base(engine)

# -- Person Model --
class Person(Base):
    __tablename__ = "people"

    id = Column(Integer, primary_key=True)
    names = Column(String)
    father_name = Column(String)
    mother_name = Column(String)
    sex = Column(String)
    born_date = Column(DateTime)
    grade = Column(String)

    arrests = relationship("Arrest")

# -- Arrest Model --
class Arrest(Base):
    __tablename__ = "arrests"

    id = Column(Integer, primary_key=True)
    arrest_date = Column(DateTime)

    person_id = Column(Integer, ForeignKey('people.id'))
    prosecutor_id = Column(Integer, ForeignKey('prosecutors.id'))
    crime_id = Column(Integer, ForeignKey('crimes.id'))

# -- Crime Model --
class Crime(Base):
    __tablename__ = "crimes"

    id = Column(Integer, primary_key=True)
    description = Column(String)

# -- Office Model --
class Office(Base):
    __tablename__ = "offices"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    code = Column(String)
    prosecutors = relationship("Prosecutor", backref="office")

    def __repr__(self):
        return "<Office(name='%s', code='%s')>" % ( self.name, self.code)

    def is_ready(self, arg):
        return arg

# -- Prosecutor Model --
class Prosecutor(Base):
    __tablename__ = "prosecutors"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    arrests = relationship("Arrest")
    office_id = Column(Integer, ForeignKey('offices.id'))
    # office = relationship("Office", backref=backref('prosecutors', order_by=id))

# -- Prosecutor Model --
class Item(Base):
    __tablename__ = "items"

    id = Column(Integer, primary_key=True)
    dbf_id = Column(String)
    office = Column(String)
    register_date = Column(DateTime)
    import_date = Column(DateTime)
