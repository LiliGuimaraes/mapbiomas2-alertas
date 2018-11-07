#!/usr/bin/python

import io
import ogr
import osr
import zipfile
import requests
from datetime import datetime

DS_NAME="baseline/SAD.shp"
#DS_CON_ALERTS='PG:dbname=mapbiomas_alertas host=35.231.230.67 user=postgres password=5Pdd444m76t8ZcA'
DS_CON_ALERTS='PG:dbname=mapbiomas_alertas host=localhost user=postgres'

ALERTS_SOURCE='SAD'
ALERTS_TMP_TABLE="consolidated_alerts_tmp_sad"

def current_date():
	return datetime.now().strftime('%Y/%m/%d')

def str_to_date(date_str, format='%Y/%m/%d'):
	return datetime.strptime(date_str,format).date()

def last_date_alert(postgis_ds, source_name):
	last_date_sql = "SELECT max(detection_date) AS last_date FROM consolidated_alerts WHERE source = '"+source_name+"'"
	rows = postgis_ds.ExecuteSQL(last_date_sql)
	last_date_str = rows[0].last_date

	return str_to_date(last_date_str)

def download_and_unzip(url, destination_path):
	response = requests.get(url)
	with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
		zf.extractall(destination_path)
	
postgis_drv = ogr.GetDriverByName("PostgreSQL")
postgis_ds = postgis_drv.CreateDataSource(DS_CON_ALERTS)
tmp_alerts_layer = postgis_ds.GetLayerByName(ALERTS_TMP_TABLE)

#last_date = last_date_alert(postgis_ds, ALERTS_SOURCE)
#print("Most recent " + ALERTS_SOURCE + " alert:" + str(last_date))

input_ds = ogr.Open(DS_NAME)
input_layer = input_ds.GetLayer()

input_srs = input_layer.GetSpatialRef()
consolidated_alerts_srs = tmp_alerts_layer.GetSpatialRef()
transformation_srs = osr.CoordinateTransformation(input_srs, consolidated_alerts_srs)

for input_feature in input_layer:
	month = input_feature.GetField("Mes")
	year = input_feature.GetField("Ano")
	feature_date_str = str(year)+'/'+str(month)+'/01'
	feature_date = str_to_date(feature_date_str)
	#if(feature_date >= last_date):
	new_alert_feature = ogr.Feature(tmp_alerts_layer.GetLayerDefn())
	
	input_geometry = input_feature.GetGeometryRef()
	input_geometry.Transform(transformation_srs)
	new_alert_feature.SetGeometry(ogr.ForceToMultiPolygon(input_geometry))

	new_alert_feature.SetField('detection_date', feature_date_str)
	new_alert_feature.SetField('sensor', input_feature.GetField("Sensor"))
	new_alert_feature.SetField('source', ALERTS_SOURCE)
	new_alert_feature.SetField('insertion_date', current_date())
	
	tmp_alerts_layer.CreateFeature(new_alert_feature)
	print("Insert new " + ALERTS_SOURCE + " alert:" + str(feature_date))
		