#!/usr/bin/python

import io
import ogr
import osr
import zipfile
import requests
from datetime import datetime

URL_DOWNLOAD='http://terrabrasilis.info/files/deter_cerrado/deter_cerrado_all.zip'
DS_NAME="baseline/DETER-CERRADO.shp"
DS_CON_ALERTS='PG:dbname=mapbiomas_alertas host=localhost user=postgres'

ALERTS_SOURCE='DETER-CERRADO'
ALERTS_TMP_TABLE="consolidated_alerts_tmp_deter_cerrado"

def current_date():
	return datetime.now().strftime('%Y/%m/%d')

def str_to_date(date_str):
	return datetime.strptime(date_str,'%Y/%m/%d').date()

def last_date_alert(postgis_ds, source_name):
	last_date_sql = "SELECT max(detection_date) AS last_date FROM consolidated_alerts WHERE source = '"+source_name+"'"
	rows = postgis_ds.ExecuteSQL(last_date_sql)
	last_date_str = rows[0].last_date

	return str_to_date(last_date_str)

def download_and_unzip(url, destination_path):
	response = requests.get(url)
	with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
		zf.extractall(destination_path)
	
#print("Downloading file from " + URL_DOWNLOAD)
#download_and_unzip(URL_DOWNLOAD, '.')

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
	feature_date_str = input_feature.GetField("VIEW_DATE")
	feature_date = str_to_date(feature_date_str)
	#if(feature_date >= last_date):
	new_alert_feature = ogr.Feature(tmp_alerts_layer.GetLayerDefn())
	
	input_geometry = input_feature.GetGeometryRef()
	input_geometry.Transform(transformation_srs)
	new_alert_feature.SetGeometry(ogr.ForceToMultiPolygon(input_geometry))

	new_alert_feature.SetField('detection_date', input_feature.GetField("VIEW_DATE"))
	new_alert_feature.SetField('sensor', input_feature.GetField("SENSOR"))
	new_alert_feature.SetField('source', ALERTS_SOURCE)
	new_alert_feature.SetField('insertion_date', current_date())
	
	tmp_alerts_layer.CreateFeature(new_alert_feature)
	print("Insert new " + ALERTS_SOURCE + " alert:" + str(feature_date))
		