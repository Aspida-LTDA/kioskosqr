import psycopg2
import requests
import json
import xlsxwriter

base_url = "https://apikioskosaspida.azurewebsites.net/"

connectionParams = [
                    #{'stationHost': '10.212.134.197', 'stationPass': 'aspida123', 'stationDBName': 'BiciparkingGeneralSantander'},
                    #{'stationHost': '10.212.134.200', 'stationPass': 'aspida***', 'stationDBName': 'BiciparkingSanMateo'},
                    {'stationHost': '10.212.134.202', 'stationPass': 'aspida***', 'stationDBName': 'BiciparkingTunal'},
                    
                    #{'stationHost': '10.212.134.196', 'stationPass': 'aspida***', 'stationDBName': 'BiciparkingDorado'},
                    #{'stationHost': '10.212.134.195', 'stationPass': 'aspida***', 'stationDBName': 'BiciparkingBanderas'},
                    #{'stationHost': '10.212.134.194', 'stationPass': 'aspida123', 'stationDBName': 'BiciparkingPortalAmericas'},
                    
                    {'stationHost': '10.212.134.193', 'stationPass': 'aspida***', 'stationDBName': 'BiciparkingAVRojas'},
                    {'stationHost': '10.212.134.201', 'stationPass': 'aspida***', 'stationDBName': 'BiciparkingSuba'},
                    {'stationHost': '10.212.134.198', 'stationPass': 'aspida***', 'stationDBName': 'BiciparkingPortal80'},
                    {'stationHost': '10.212.134.199', 'stationPass': 'aspida***', 'stationDBName': 'BiciparkingPortalSur'}]

def create_cyclist(cyclist, cursor, arreglo400, arreglo200):
    url = base_url + "cyclist/"

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.request("POST", url, headers=headers, data=cyclist)
    if response.status_code == 201:
        updatequery = 'UPDATE ct_pn_ps SET synchronized=1 WHERE cyclist_id=%s AND id_type_id=%s'
        arreglo200 = arreglo200.append(cyclist["cyclist_id"])
    elif response.status_code == 400:
        updatequery = 'UPDATE ct_pn_ps SET synchronized=400 WHERE cyclist_id=%s AND id_type_id=%s'
        arreglo400 = arreglo400.append(cyclist["cyclist_id"])
    else:
        updatequery = 'UPDATE ct_pn_ps SET synchronized=300 WHERE cyclist_id=%s AND id_type_id=%s'
    #print("*******", arreglo400, "******", response.status_code)
    #return arreglo400, arreglo200
    try:
        cursor.execute(updatequery, [cyclist["cyclist_id"], cyclist["id_type_id"]])
        #print("Afectadas: ")
    except Exception as err:
        print("Oops! An exception has occured:", err)
        print("Exception TYPE:", type(err))

    print_response(response)

def create_bike(bike, cursor, arreglo400, arreglo200):
    url = base_url + "bike/"

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.request("POST", url, headers=headers, data=bike)
    json1 = response.json()
    if(json1.get('bike_code') is not None):
        if(json1.get('bike_code')>0):
            syncupdate = json1.get('bike_code')
        else:
            syncupdate = -400
    else:
        syncupdate = -300
    #print("----", syncupdate)
    if response.status_code == 201:
        updatequery = 'UPDATE be_ve_ps SET synchronized=%s WHERE bike_code=%s'
        arreglo200 = arreglo200.append(bike["bike_serial"])
    elif response.status_code == 400:
        updatequery = 'UPDATE be_ve_ps SET synchronized=%s WHERE bike_code=%s'
        arreglo400 = arreglo400.append(bike["bike_serial"])
    else:
        updatequery = 'UPDATE be_ve_ps SET synchronized=%s WHERE bike_code=%s'
    #print("*******", arreglo400, "******", response.status_code)
    #return arreglo400, arreglo200
    try:
        #print("antes")
        cursor.execute(updatequery, [syncupdate, bike["bike_serial"]])
        #print(updatequery, bike["bike_serial"])
    except Exception as err:
        print("Oops! An exception has occured:", err)
        print("Exception TYPE:", type(err))

    print_response(response)

def create_event(event, cursor, arreglo400, arreglo200):
    url = base_url + "bikeEvent/"

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.request("POST", url, headers=headers, data=event)
    #print(response)
    if response.status_code == 201:
        updatequery = 'UPDATE be_et_ps SET synchronized=1 WHERE bike_event_id=%s'
        arreglo200 = arreglo200.append(event["id"])
    elif response.status_code == 400:
        updatequery = 'UPDATE be_et_ps SET synchronized=400 WHERE bike_event_id=%s'
        arreglo400 = arreglo400.append(event["id"])
    else:
        updatequery = 'UPDATE be_et_ps SET synchronized=300 WHERE bike_event_id=%s'
    #print("*******", arreglo400, "******", response.status_code)
    try:
        cursor.execute(updatequery, [event["id"]])
        #print("Afectadas: ")
        #print(event["id"])
    except Exception as err:
        print("Oops! An exception has occured:", err)
        print("Exception TYPE:", type(err))

    print_response(response)

def print_response(response):
    print(response.text)
    #print(response.json())
    print(response.status_code)
    #print(response.headers)

for parametros in connectionParams:
    print("---")
    print(parametros['stationDBName'])

    conexion = psycopg2.connect(
        user='postgres',
        password=parametros['stationPass'],
        host=parametros['stationHost'],
        port='5432',
        database=parametros['stationDBName']
    )
    workbook = xlsxwriter.Workbook(parametros['stationDBName']+'.xlsx')
 
    worksheet = workbook.add_worksheet()
    worksheet.write('A1', 'FECHA')
    worksheet.write('B1', 'HORA')
    worksheet.write('C1', 'EVENTO')
    worksheet.write('D1', 'IDENTIFICACION')
    worksheet.write('E1', 'NOMBRES')
    worksheet.write('F1', 'APELLIDOS')
    worksheet.write('G1', 'COLOR')
    worksheet.write('H1', 'SERIAL')
    worksheet.write('I1', 'TIPO')
    worksheet.write('J1', 'MARCA')
    worksheet.write('K1', 'OBSERVACIONES')
    
    try:
        with conexion:
            with conexion.cursor() as cursor:
                #Ciclistas
                sentencia = "SELECT TO_CHAR(be_et_ps.event_date, 'yyyy-mm-dd'), TO_CHAR(be_et_ps.event_date, 'hh24:mm:ss'), et_te_ps.event_type, ct_pn_ps.cyclist_id, CONCAT(ct_pn_ps.first_name, ' ', ct_pn_ps.second_name), CONCAT(ct_pn_ps.first_last_name, ' ', ct_pn_ps.second_last_name),\
                be_cr_ps.color_name, be_ve_ps.bike_serial, be_te_ps.type_name, be_bd_ps.brand, \
		        be_et_ps.notes	FROM ct_pn_ps JOIN be_ve_ps\
                ON (ct_pn_ps.cyclist_id=be_ve_ps.cyclist_id AND ct_pn_ps.id_type_id=be_ve_ps.cy_id_type_id) JOIN be_te_ps\
	            ON be_ve_ps.bike_type_id=be_te_ps.type_id JOIN be_cr_ps\
	            ON be_ve_ps.color_id=be_cr_ps.color_id JOIN be_bd_ps\
	            ON be_ve_ps.brand_id=be_bd_ps.id_brand JOIN be_et_ps\
	            ON be_ve_ps.bike_code=be_et_ps.bike_id JOIN et_te_ps\
	            ON be_et_ps.event_type_id=et_te_ps.event_type_id \
	            WHERE event_date>'2024-04-01' AND event_date<'2024-04-16'\
	            ORDER BY event_date"
                contador = 0
                cursor.execute(sentencia)
                registros = cursor.fetchall()
                total = len(registros)
                

                if total > 0:
                    var400 = []
                    var200 = []
                    retorno = []
                    for registro in registros:
                        contador=contador+1
                        fila = str(contador+1)
                        worksheet.write('A'+fila, registro[0])
                        worksheet.write('B'+fila, registro[1])
                        worksheet.write('C'+fila, registro[2])
                        worksheet.write('D'+fila, registro[3])
                        worksheet.write('E'+fila, registro[4])
                        worksheet.write('F'+fila, registro[5])
                        worksheet.write('G'+fila, registro[6])
                        worksheet.write('H'+fila, registro[7])
                        worksheet.write('I'+fila, registro[8])
                        worksheet.write('J'+fila, registro[9])
                        worksheet.write('K'+fila, registro[10])
                        print(contador)
                        print(registro)

    except Exception as e:
        print(f'Ocurrió un error: {e}')
    finally:
        cursor.close()
    
    workbook.close()





"""
conexion = psycopg2.connect(
    user='postgres',
    password='aspida***',
    host='10.212.134.193',
    port='5432',
    database='BiciparkingAVRojas'
)
try:
    with conexion:
        with conexion.cursor() as cursor:
            #Ciclistas
            sentencia = 'SELECT * FROM ct_pn_ps WHERE synchronized=10 LIMIT 100'
            contador = 0
            cursor.execute(sentencia)
            registros = cursor.fetchall()
            total = len(registros)
            if total > 0:
                var400 = []
                var200 = []
                retorno = []
                for registro in registros:
                    contador=contador+1
                    print(contador)
                    secondlastname =registro[7]
                    if registro[7] == '':
                        secondlastname = '-'
                    secondname = registro[5]
                    if registro[5] == '':
                        secondname = '-'
                    cyclist = {
                        "cyclist_id": registro[0],
                        "first_name": registro[4],
                        "second_name": secondname,
                        "first_last_name": registro[6],
                        "second_last_name": secondlastname,
                        "birth_date": registro[8],
                        "mobile_number": registro[0],
                        "address": registro[10],
                        "st": registro[11],
                        "career": registro[12],
                        "cyclist_pic": "",
                        "finger_print": registro[14],
                        "email": registro[15],
                        "kiosco_id": registro[18],
                        "id_type_id": registro[1],
                        "sex_id": registro[2],
                        "rh_id": registro[16]
                    }
                    print(cyclist)
                    create_cyclist(cyclist, cursor, var400, var200)
            else :
                sentenciaciclas = 'SELECT * FROM be_ve_ps WHERE synchronized=10 LIMIT 400'
                cursor.execute(sentenciaciclas)
                registros = cursor.fetchall()
                total = len(registros)
                if total > 0:
                    contador=0
                    var400 = []
                    var200 = []
                    retorno = []
                    for registro in registros:
                        contador=contador+1
                        bike = {
                            "kiosco_id": registro[11],
                            "bike_serial": registro[0],
                            "notes": registro[8],
                            "bike_type_id": registro[1],
                            "brand_id": registro[6],
                            "color_id": registro[2],
                            "cy_id_type_id": registro[5],
                            "cyclist_id": registro[4],
                            "model_id": registro[3]
                        }
                        print(contador)
                        print(bike)
                        create_bike(bike, cursor, var400, var200)
                else : 
                    sentenciaeventos = "SELECT * FROM be_et_ps  WHERE synchronized=0 and event_date >  '2024-01-31' order by event_date ASC LIMIT 50"
                    #print(sentenciaeventos)
                    cursor.execute(sentenciaeventos)
                    registros = cursor.fetchall()
                    total = len(registros)
                    if total > 0:
                        var400 = []
                        var200 = []
                        retorno = []
                        contador=0
                        for registro in registros:
                            #print(registro)
                            contador=contador+1
                            event = {
                                "station_id": registro[6],
                                "event_type_id": registro[1],
                                "bike_id": registro[2],
                                "date_event": registro[3],
                                "notes": registro[4],
                                "id": registro[0]
                            }
                            print(contador)
                            print(event)
                            create_event(event, cursor, var400, var200)

                

                #if total > 0:

except Exception as e:
    print(f'Ocurrió un error: {e}')
finally:
    cursor.close()"""


