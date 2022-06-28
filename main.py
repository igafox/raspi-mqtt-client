from asyncio import open_connection
from sqlite3 import Timestamp
from tkinter import E
from xmlrpc import client
import RPi.GPIO as GPIO
from numpy import result_type
import dht11
import time
import ssl
import datetime
import json
import schedule

import jwt
import paho.mqtt.client as mqtt 

#ピン初期化
GPIO.setwarnings(True)
GPIO.setmode(GPIO.BCM)

SENSOR_PIN = 14
SENSOR_READ_RETRY_COUNT = 5

#温湿度クライアント
instance = dht11.DHT11(pin=SENSOR_PIN) 

#MQTT認証情報
MQTT_BROKER_HOST = "mqtt.googleapis.com"
MQTT_BROKER_PORT = 8883

CA_CERTS = "roots.pem"
PROJECT_ID = ""
CLOUD_REGION = "asia-east1"
REGISTRY_ID = "main-registry"
DEVICE_ID = "raspi"
PRIVATE_KEY_FILE = "rsa_private.pem"
ALGORITHM= "RS256"
CLIENT_ID = "projects/{}/locations/{}/registries/{}/devices/{}".format(
	PROJECT_ID, CLOUD_REGION, REGISTRY_ID, DEVICE_ID)	

MQTT_TOPIC = "/devices/{}/{}".format(DEVICE_ID, "events")

JWT_EXP_HOUR = 24

mqtt_client = None

jwt_iat = None
jwt_exp = None

def on_connect(unused_client, unused_userdata, unused_flags, rc):
    print("MQTT接続結果:{}".format(mqtt.connack_string(rc)))

def on_disconnect(unused_client, unused_userdata, rc):
	print("MQTT切断:{}".format(mqtt.error_string(rc)))

def on_publish(unused_client, unused_userdata, unused_mid):
    print("MQTT Publish成功")

def createToken():
	global jtw_iat,jwt_exp
	jwt_iat = datetime.datetime.now(tz=datetime.timezone.utc)
	jwt_exp = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(hours=JWT_EXP_HOUR)

	token = {
		"iat": jwt_iat,
		"exp": jwt_exp,
		"aud": PROJECT_ID,
		}
		
	with open(PRIVATE_KEY_FILE, "r") as f:
		private_key = f.read()
		
	token = jwt.encode(token, private_key, algorithm=ALGORITHM)
	return token

def setupMqtt():
	print("MQTT初期化開始")
	#MQTTクライアント初期化	
	global mqtt_client
	mqtt_client = mqtt.Client(client_id=CLIENT_ID)
	mqtt_client.username_pw_set(
		username="unused", password=createToken()
		)
	mqtt_client.tls_set(ca_certs=CA_CERTS, tls_version=ssl.PROTOCOL_TLSv1_2)
    
	#コールバック関数設定
	mqtt_client.on_connect = on_connect
	mqtt_client.on_publish = on_publish
	mqtt_client.on_disconnect = on_disconnect
	
	print("MQTT接続開始")
	#MQTT接続
	mqtt_client.connect(MQTT_BROKER_HOST,MQTT_BROKER_PORT)
	mqtt_client.loop_start()

def publishSensorData():
	global mqtt_client,exp

	#日付取得
	timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
	timestamp_text = timestamp.strftime('%Y-%m-%d %H:%M:%S')

	#センサー読み取り
	for i in range(1, SENSOR_READ_RETRY_COUNT + 1):
		result = instance.read()
		if result.is_valid() :
			break
		else:
			#失敗した場合、リトライ
			time.sleep(3)

	if not result.is_valid():
		#リトライ失敗
		print("測定エラー")
		return

	#データ整形
	temperature = "%-3.1f" % result.temperature
	humidity = "%-3.1f" % result.humidity

	print("取得時間:{} 温度:{} °C 湿度:{} ％".format(
		timestamp.strftime('%Y/%m/%d %H:%M:%S'),temperature,humidity))

    #送信データ作成
	payload = {
		"deviceId":DEVICE_ID,
		"time": timestamp_text,
		"temperature":temperature,
		"humidity": humidity
		}
		
	#JWTトークン有効期限チェック
	if jwt_exp <= timestamp :
		print("JWTトークン更新")
		mqtt_client.disconnect()
		setupMqtt()

    #MQTT送信
	mqtt_client.publish(
		MQTT_TOPIC,
		str(json.dumps(payload)),
		qos=1)	
		

def main():	

    #MQTT初期化
	setupMqtt()
		
	schedule.every().minute.at(":00").do(publishSensorData)
	# schedule.every(10).seconds.do(publishSensorData)

	while True:
		schedule.run_pending()
		time.sleep(1)

if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		print("clean up")
		GPIO.cleanup()