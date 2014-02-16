# -*- coding: utf-8 -*-

from model import Office, Prosecutor, Person, Crime, Arrest, Item

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

class Offices(object):
    """docstring for Offices"""
    def __init__(self, session):
        super(Offices, self).__init__()
        self.session = session

    def insert(self, name):
        session = self.session

        query = session.query(Office).filter(Office.name == name)
        query_exist = session.query(query.exists())

        if query_exist.scalar():
            office = query.first()
            return office.id
        else:
            try:
                new_office = Office(name=name)
                session.add(new_office)
                session.commit()
            except Exception, e:
                raise e
                print Exception

            return new_office.id

class Prosecutors(object):
    """docstring for Prosecutors"""
    def __init__(self, session):
        super(Prosecutors, self).__init__()
        self.session  = session

    def insert(self, name, office_id):
        session = self.session

        query = session.query(Prosecutor).filter(Prosecutor.name == name, Prosecutor.office_id == office_id)
        query_exist = session.query(query.exists())

        if query_exist.scalar():
            prosecutor = query.first()
            return prosecutor.id
        else:
            try:
                new_prosecutor = Prosecutor(name=name, office_id=office_id)
                session.add(new_prosecutor)
                session.commit()
            except Exception, e:
                raise e
                print Exception

            return new_prosecutor.id

class People(object):
    """docstring for People"""
    def __init__(self, session):
        super(People, self).__init__()
        self.session = session

    def insert(self, arg):
        session = self.session

        query = session.query(Person).filter(Person.names == arg['names'], Person.father_name == arg['father_name'], Person.mother_name == arg['mother_name'])
        query_exist = session.query(query.exists())

        if query_exist.scalar():
            person = query.first()
            return person.id
        else:
            try:
                new_person = Person(names=arg['names'], father_name=arg['father_name'], mother_name=arg['mother_name'], sex=arg['sex'], grade=arg['grade'], born_date=arg['born_date'])

                session.add(new_person)
                session.commit()
            except Exception, e:
                raise e
                print Exception

            return new_person.id

class Crimes(object):
    """docstring for Crimes"""
    def __init__(self, session):
        super(Crimes, self).__init__()
        self.session = session

    def insert(self, description):
        session = self.session

        query = session.query(Crime).filter(Crime.description == description)
        query_exist = session.query(query.exists())

        if query_exist.scalar():
            crime = query.first()
            return crime.id
        else:
            try:
                new_crime = Crime(description=description)
                session.add(new_crime)
                session.commit()
            except Exception, e:
                raise e
                print Exception

            return new_crime.id

class Arrests(object):
    """docstring for Arrests"""
    def __init__(self, session):
        super(Arrests, self).__init__()
        self.session = session

    def insert(self, arg):
        session = self.session
        try:
            arrest = Arrest(person_id=arg['person_id'], prosecutor_id=arg['prosecutor_id'], crime_id=arg['crime_id'], arrest_date=arg['arrest_date'])
            session.add(arrest)
            session.commit()
            return True

        except Exception, e:
            print Exception
            raise

        return False

class Items(object):
    def __init__(self, session):
        super(Offices, self).__init__()
        self.session = session

    def exist(self, arg):
        session = self.session

        query = session.query(Item).filter(Item.dbf_id == arg['dbf_id'], Item.office  == arg['office'], Item.register_date == arg['register_date'])
        query_exist = session.query(query.exists())
        return query_exist.scalar()

    def insert(self, arg):
        try:
            new_office = Item(dbf_id=arg['dbf_id'], office=Item['office'], register_date=arg['register_date'])
            session.add(new_office)
            session.commit()
        except Exception, e:
            raise e
            print Exception

class Pakages(object):
    """docstring for Pakages"""
    def __init__(self):
        super(Pakages, self).__init__()
        self.import_dbf()

    def connect_database(self):
        engine = create_engine('mysql+mysqldb://root:123123_@127.0.0.1:3306/detenidos_db?charset=utf8', echo=False)
        Session = sessionmaker(bind=engine)
        session  = Session()
        return session

    def import_dbf(self):
        session = self.connect_database()

        # Bucle of dbf rows

        # (1) Step - Offices & Prosecutors
        prosecutors = Prosecutors(session)
        offices = Offices(session)
        people = People(session)
        crimes = Crimes(session)
        arrests = Arrests(session)

        try:

            office_id = offices.insert(name="Oficina Opp")
            prosecutor_id = prosecutors.insert(name="1° Fiscalia ", office_id=office_id)
            person_id = people.insert({'names': 'joseph', 'father_name': 'asd', 'mother_name': 'asd', 'sex': 'F', 'grade': 'primaria', 'born_date': '2005-10-03'})
            crime_id = crimes.insert(description="Robo")

            # insert Arrest
            result = arrests.insert({'person_id': int(person_id), 'prosecutor_id': int(prosecutor_id), 'crime_id': int(crime_id), 'arrest_date': '2005-10-03' })

            print "office_id:" , office_id
            print "prosecutor_id:", prosecutor_id
            print "person_id:", person_id
            print "crime_id:", crime_id

            print result

        except:
            session.rollback()
            raise
        finally:
            session.close()

Pakages()
