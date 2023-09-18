from argparse import ArgumentError
import ssl
from django.db.models import Avg
from datetime import timedelta, datetime
from receiver.models import Data, Measurement
import paho.mqtt.client as mqtt
import schedule
import time
from django.conf import settings
import dateutil.relativedelta

client = mqtt.Client(settings.MQTT_USER_PUB)


def analyze_data():
    # Consulta todos los datos de la última hora, los agrupa por estación y variable
    # Compara el promedio con los valores límite que están en la base de datos para esa variable.
    # Si el promedio se excede de los límites, se envia un mensaje de alerta.

    print("Calculando alertas...")

    data = Data.objects.filter(base_time__gte=datetime.now() - timedelta(hours=1))
    aggregation = data.annotate(check_value=Avg('avg_value')) \
        .select_related('station', 'measurement') \
        .select_related('station__user', 'station__location') \
        .select_related('station__location__city', 'station__location__state',
                        'station__location__country') \
        .values('check_value', 'station__user__username',
                'measurement__name',
                'measurement__max_value',
                'measurement__min_value',
                'station__location__city__name',
                'station__location__state__name',
                'station__location__country__name')
    alerts = 0
    for item in aggregation:
        alert = False

        variable = item["measurement__name"]
        max_value = item["measurement__max_value"] or 0

        min_value = item["measurement__min_value"] or 0

        country = item['station__location__country__name']
        state = item['station__location__state__name']
        city = item['station__location__city__name']
        user = item['station__user__username']
        
        if variable != "bateria":
            if item["check_value"] > max_value or item["check_value"] < min_value:
                alert = True
        else:
            if item["check_value"] < min_value:
                alert = True
                batteryLastData = Data.objects.filter(measurement = item['measurement__name']).order_by('-time')[:2]
                print("BATTERY DATA:")
                print(batteryLastData)
                if batteryLastData[1]["value"] > batteryLastData[0]["value"]:
                    # ALARMA ADICIONAL PARA RECORDAR RECARGAR BATERÍA PORQUE NO LO HA HECHO:
                    print("BATERÍA BAJA Y DESCONECTADA DE LA ENERGÍA")
                    message = "ALERT: CHARGE THE BATTERY NOW!"
                    topic = '{}/{}/{}/{}/in'.format(country, state, city, user)
                    print(datetime.now(), "Sending alert to {} suggesting the charge of the battery".format(topic))
                    client.publish(topic, message)
                else:
                    print("BATERÍA RECARGANDO")

        if alert:
            message = "ALERT {} {} {}".format(variable, min_value, max_value)
            topic = '{}/{}/{}/{}/in'.format(country, state, city, user)
            print(datetime.now(), "Sending alert to {} {}".format(topic, variable))
            client.publish(topic, message)
            alerts += 1

    print(len(aggregation), "dispositivos revisados")
    print(alerts, "alertas enviadas")


def on_connect(client, userdata, flags, rc):
    '''
    Función que se ejecuta cuando se conecta al bróker.
    '''
    print("Conectando al broker MQTT...", mqtt.connack_string(rc))


def on_disconnect(client: mqtt.Client, userdata, rc):
    '''
    Función que se ejecuta cuando se desconecta del broker.
    Intenta reconectar al bróker.
    '''
    print("Desconectado con mensaje:" + str(mqtt.connack_string(rc)))
    print("Reconectando...")
    client.reconnect()


def setup_mqtt():
    '''
    Configura el cliente MQTT para conectarse al broker.
    '''

    print("Iniciando cliente MQTT...", settings.MQTT_HOST, settings.MQTT_PORT)
    global client
    try:
        client = mqtt.Client(settings.MQTT_USER_PUB)
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect

        if settings.MQTT_USE_TLS:
            client.tls_set(ca_certs=settings.CA_CRT_PATH,
                           tls_version=ssl.PROTOCOL_TLSv1_2, cert_reqs=ssl.CERT_NONE)

        client.username_pw_set(settings.MQTT_USER_PUB,
                               settings.MQTT_PASSWORD_PUB)
        client.connect(settings.MQTT_HOST, settings.MQTT_PORT)

    except Exception as e:
        print('Ocurrió un error al conectar con el bróker MQTT:', e)


def start_cron():
    '''
    Inicia el cron que se encarga de ejecutar la función analyze_data cada 5 minutos.
    '''
    print("Iniciando cron...")
    schedule.every(1).minutes.do(analyze_data)
    print("Servicio de control iniciado")
    while 1:
        schedule.run_pending()
        time.sleep(1)
        
        

def get_last_context(countryParam,stateParam,cityParam,userParam):
    """
Se procesan los datos para cargar el contexto del template.
El template espera un contexto de este tipo:
{
    "data": {
        "temperatura": {
            "min": float,
            "max": float,
            "avg": float,
            "data": [
                (timestamp1, medición1),
                (timestamp2, medición2),
                (timestamp3, medición3),
                ...
            ]
        },
        "variable2" : {min,max,avg,data},
        ...
    },
    "measurements": [Measurement0, Measurement1, ...],
}
"""
    context = {}
    try:
        context["data"], context["measurements"] = get_last_week_data(
            userParam, cityParam, stateParam, countryParam
        )
    except Exception as e:
        print("Error get_context_data. User: " + userParam, e)
    return context


def get_last_week_data(user, city, state, country):
    result = {}
    start = datetime.now()
    start = start - dateutil.relativedelta.relativedelta(days=1)
    try:
        userO = User.objects.get(username=user)
        location = None
        try:
            cityO = City.objects.get(name=city)
            stateO = State.objects.get(name=state)
            countryO = Country.objects.get(name=country)
            location = Location.objects.get(
                city=cityO, state=stateO, country=countryO
            )
        except:
            print("Specified location does not exist")
        print("LAST_WEEK: Got user and lcoation:",
              user, city, state, country)
        if userO == None or location == None:
            raise "No existe el usuario o ubicación indicada"
        stationO = Station.objects.get(user=userO, location=location)
        print("LAST_WEEK: Got station:", user, location, stationO)
        if stationO == None:
            raise "No hay datos para esa ubicación"
        measurementsO = get_measurements()
        print("LAST_WEEK: Measurements got: ", measurementsO)
        for measure in measurementsO:
            print("LAST_WEEK: Filtering measure: ", measure)
            # time__gte=start.date() Filtro para último día
            start_ts = int(start.timestamp() * 1000000)
            raw_data = Data.objects.filter(
                station=stationO, time__gte=start_ts, measurement=measure
            ).order_by("-base_time")[:2]
            print("LAST_WEEK: Raw data: ", len(raw_data))
            data = []
            for reg in raw_data:
                values = reg.values
                times = reg.times
                print("Len vals: ", len(values), "Len times: ", len(times))
                for i in range(len(values)):
                    data.append(
                        (
                            ((reg.base_time.timestamp() +
                             times[i]) * 1000 // 1),
                            values[i],
                        )
                    )

            minVal = raw_data.aggregate(Min("min_value"))["min_value__min"]
            maxVal = raw_data.aggregate(Max("max_value"))["max_value__max"]
            avgVal = sum(reg.avg_value * reg.length for reg in raw_data) / sum(
                reg.length for reg in raw_data
            )
            result[measure.name] = {
                "min": minVal if minVal != None else 0,
                "max": maxVal if maxVal != None else 0,
                "avg": round(avgVal if avgVal != None else 0, 2),
                "data": data,
            }
    except Exception as error:
        print("Error en consulta de datos:", error)
        traceback.print_exc()

    return result, measurementsO
