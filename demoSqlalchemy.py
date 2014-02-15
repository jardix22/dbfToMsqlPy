# -*- coding: utf-8 -*- 

from model import Office

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

class Collection(object):
    """docstring for Collection"""
    def __init__(self):
        super(Collection, self).__init__()
        #self.model = model

    def connect_database(self):
        engine = create_engine('mysql+mysqldb://root:123123_@127.0.0.1:3306/detenidos_db?charset=utf8', echo = False)
        Session = sessionmaker(bind=engine)
        session  = Session()        
        return session

class Offices(Collection):
    """docstring for Offices"""
    def __init__(self):
        super(Offices, self).__init__()
         # self.arg = arg

    def if_exist(self, name):
        session = self.connect_database()
        query = session.query(Office).filter(Office.name == name)
        query_exist = session.query(query.exists())
        session.close()

        return query_exist.scalar()

    def insert_if_not_exist(arg):
        if not(self.if_exist()):
            try:
                session = self.connect_database()
                office = Office(name=arg.name, code=arg.code)
                session.add(new_office)
                session.commit()
            except e
                print "Office can't insert " + e

            session.close()
            return True
        else:
            print "Office: %s", arg.name
            return False

class Pakages(object):
    """docstring for Pakages"""
    def __init__(self):
        super(Pakages, self).__init__()
        # self.import_row()

    def import_row(self):
        #session = self.connect_database()

        offices = Offices()
        print offices.if_exist("Oficina Lima")

        #try:
        #    query = session.query(Office).filter(Office.code.in_(['010'])).one()
        #    print query
        #    a = query.is_ready(12)
        #    print a
        #except NoResultFound, e:
        #    print e
        
        #print "Not found!!"
        #session.close()

    #def connect_database(self):
    #    engine = create_engine('mysql+mysqldb://root:123123_@127.0.0.1:3306/detenidos_db?charset=utf8', echo = True)
    #    Session = sessionmaker(bind=engine)
    #    session  = Session()
    #    return session
        
# engine.connect()
# Session.configure()

# -- Add Office
#new_office = Office(name='Oficina Lima', code='014')
#session.add(new_office)
#session.commit()

# for isntance in session.query(Office).filter(Office.code.in_(['010'])).order_by(Office.id):
#   print isntance.id, isntance.name, isntance.code

pakages = Pakages()
pakages.import_row()