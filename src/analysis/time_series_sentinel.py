import json
import ee
import datetime
import ogr
import psycopg2

# 2:20hr
# Gid 56.714

ee.Initialize()

DS_CON_ALERTS='PG:dbname=mapbiomas_alertas host=localhost user=postgres'
postgis_drv = ogr.GetDriverByName("PostgreSQL")
postgis_ds = postgis_drv.CreateDataSource(DS_CON_ALERTS)

con = psycopg2.connect(dbname="mapbiomas_alertas",host='localhost', user='postgres', password='postgres')
# Get the database cursor to execute queries
cur = con.cursor()

sql = "SELECT id, ST_ASGEOJSON(ST_Collect(ST_MakePolygon(geom))) AS geojson \
        FROM ( \
            SELECT id, ST_ExteriorRing((ST_Dump(geom)).geom) As geom \
            FROM consolidated_alerts \
            ORDER BY detection_date DESC \
            ) s \
        GROUP BY id"

rows = postgis_ds.ExecuteSQL(sql)

for row in rows:
  
  geometry = ee.Geometry.MultiPolygon(json.loads(row.geojson)['coordinates'])
  print("Consulting Sentinel TS for " + str(row.id))

  NDVI = "((b('B8') - b('B4')) /(b('B8') + b('B4')))"

  S2Coll = ee.ImageCollection('COPERNICUS/S2') \
                .filterBounds(geometry)

  def iterate_S2(img):

    mascara_nuvem = img.expression("(b('QA60') == 0)").add(img.lte(0));
    imgRef =  img.mask(mascara_nuvem);
    result = imgRef.expression(NDVI).clip(geometry).reduceRegions(geometry, ee.Reducer.mean(), 30);
    
    return ee.FeatureCollection(result).first()

  S2Coll_fill = S2Coll.map(iterate_S2).toList(S2Coll.size()).getInfo()

  def getData(feat):
    date = str(feat['id'][0:4]) +'-' + str(feat['id'][4:6]) +'-' + str(feat['id'][6:8])

    try:
      ndvi = float(feat['properties']['mean'])
    except:
      ndvi = None
    return date, ndvi

  S2_Time_Series = {
    'date': [],
    'ndvi': []
  }

  for i in range(0,len(S2Coll_fill)):
    featInfo = getData(S2Coll_fill[i])
    S2_Time_Series['date'].append(featInfo[0])
    S2_Time_Series['ndvi'].append(featInfo[1])

  insertSql = "INSERT INTO ndvi_ts(cons_alert_id,sensor,ts_data) VALUES(" + str(row.id) +", 'SENTINEL', '" + json.dumps(S2_Time_Series) + "');"
  cur.execute(insertSql)
  con.commit()