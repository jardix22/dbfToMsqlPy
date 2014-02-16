import MySQLdb
from dbfpy import dbf

# -- initialize --
dbm = MySQLdb.connect(host="localhost", user="root", passwd="123123_", db="detenidos_db")
cur = dbm.cursor()

db = dbf.Dbf("ok.DBF")


# -- Get offices --

items = []
i = 0

for rec in db:
	
	#office = rec['RN_NOMBRE'].decode('Latin-1').encode('utf-8')
	#procecutor = rec['MPF_NOMBRE'].decode('Latin-1').encode('utf-8')
	#item = {}
	
	#item['office'] = rec['RN_NOMBRE']
	#item['procecutor'] = rec['MPF_NOMBRE']

	# items.append(item)
	
	# -- Save offices in the DB --
		
	
	#else:
	#	break

	if rec['RN_NOMBRE'] != "OFICINA PUNO":
		i = i + 1

		if i % 1000 == 0:
			print i

		rec['PE_ID'] = str(rec['PE_ID'])
		rec['RN_NOMBRE'] = str(rec['RN_NOMBRE'])
		rec['DEN_PATERN'] = str(rec['DEN_PATERN'])
		rec['DEN_MATERN'] = str(rec['DEN_MATERN'])
		rec['DEN_NOMBRE'] = str(rec['DEN_NOMBRE'])
		rec['MID_FECHA'] = str(rec['MID_FECHA'])
		rec['DE_NOMBRE'] = str(rec['DE_NOMBRE'])
		rec['MPD_ID'] = str(rec['MPD_ID'])
		rec['MPF_NOMBRE'] = str(rec['MPF_NOMBRE'])
		rec['PA_NOMBRE'] = str(rec['PA_NOMBRE'])
		rec['PE_GRADO'] = str(rec['PE_GRADO'])
		rec['PE_SEXO'] = str(rec['PE_SEXO'])
		rec['PE_FNACI'] = str(rec['PE_FNACI'])
		rec['MID_EDAD'] = str(rec['MID_EDAD'])

		try:
			cur.execute("""INSERT INTO pakages (PE_ID, RN_NOMBRE, DEN_PATERN, DEN_MATERN, DEN_NOMBRE, MID_FECHA, DE_NOMBRE, MPD_ID, MPF_NOMBRE, PA_NOMBRE, PE_GRADO, PE_SEXO, PE_FNACI, MID_EDAD ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", (rec['PE_ID'], rec['RN_NOMBRE'], rec['DEN_PATERN'], rec['DEN_MATERN'], rec['DEN_NOMBRE'], rec['MID_FECHA'], rec['DE_NOMBRE'], rec['MPD_ID'], rec['MPF_NOMBRE'], rec['PA_NOMBRE'], rec['PE_GRADO'], rec['PE_SEXO'], rec['PE_FNACI'], rec['MID_EDAD']))
			dbm.commit()

		except Exception, e:
			dbm.rollback()
			print e
	
print "successfull add items"
	
#execute an sql query
#for item in items:
#print

print "successfull"
dbm.close()

	
#def saveOffices(offices):
	# -- Get offices --

#	for rec in db:
#		office = rec['RN_NOMBRE']
#		office = office.decode('Latin-1').encode('utf-8')
		
#		if not(office in offices):
#			offices.append(office)
#
#	for item in offices:
#		print item
#	print

def pushOffice():
	pass

def isInTable():
	pass