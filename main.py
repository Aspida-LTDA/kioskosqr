import psycopg2
import requests
import json

base_url = "https://apikioskosaspida.azurewebsites.net/"

connectionParams = [#{'stationHost': '10.212.134.198', 'stationPass': 'aspida***', 'stationDBName': 'BiciparkingPortal80'},
                    #{'stationHost': '10.212.134.202', 'stationPass': 'aspida***', 'stationDBName': 'BiciparkingTunal'},
                    #{'stationHost': '10.212.134.195', 'stationPass': 'aspida***', 'stationDBName': 'BiciparkingBanderas'},
                    #{'stationHost': '10.212.134.193', 'stationPass': 'aspida***', 'stationDBName': 'BiciparkingAVRojas'},
                    #{'stationHost': '10.212.134.201', 'stationPass': 'aspida***', 'stationDBName': 'BiciparkingSuba'},
                    #{'stationHost': '10.212.134.196', 'stationPass': 'aspida***', 'stationDBName': 'BiciparkingDorado'},
                    #{'stationHost': '10.212.134.197', 'stationPass': 'aspida123', 'stationDBName': 'BiciparkingGeneralSantander'},
                    #{'stationHost': '10.212.134.200', 'stationPass': 'aspida***', 'stationDBName': 'BiciparkingSanMateo'},
                    {'stationHost': '127.0.0.1', 'stationPass': '@spid@@ccess', 'stationDBName': 'BiciparkingSanMateo'},
                    #{'stationHost': '10.212.134.194', 'stationPass': 'aspida123', 'stationDBName': 'BiciparkingPortalAmericas'},
                    #{'stationHost': '10.212.134.199', 'stationPass': 'aspida***', 'stationDBName': 'BiciparkingPortalSur'}
                    ]

def create_cyclist(cyclist, cursor, arreglo400, arreglo200):
    url = base_url + "cyclist/"

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    files = {'cyclist_pic': open('C:\\Users\\Alejandro\\Downloads\\ciclista.jpg','rb')}
    print("*****")
    print(cyclist)
    response = requests.request("POST", url, headers=headers, data=cyclist, files=files)
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
    print(response)
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
    try:
        with conexion:
            with conexion.cursor() as cursor:
                #Ciclistas
                sentencia = "SELECT * FROM ct_pn_ps WHERE synchronized=0 AND cyclist_pic != '' LIMIT 1"
                
                contador = 0
                cursor.execute(sentencia)
                registros = cursor.fetchall()
                total = len(registros)
                print(registros)
                if total > 0:
                    var400 = []
                    var200 = []
                    retorno = []
                    for registro in registros:
                        contador=contador+1
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
                            "cyclist_pic": registro[13],
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
                            #create_bike(bike, cursor, var400, var200)
                    else : 
                        sentenciaeventos = "SELECT * FROM be_et_ps  WHERE synchronized=0 and event_date >  '2024-02-01' order by event_date ASC LIMIT 50"
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
                                #create_event(event, cursor, var400, var200)

                    

                    #if total > 0:

    except Exception as e:
        print(f'Ocurrió un error: {e}')
    finally:
        cursor.close()





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


