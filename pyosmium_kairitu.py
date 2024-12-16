# Data extraction from OSM using PyOsmium for Analysis
# Data is pushed to Postgres Database Using SQL Alchemy and Geo Alchemy
# Data can be saved as csv file

#!pip install PyOsmium
#!pip install wget
#!pip install geopandas
#!pip install pandas
#!pip install sqlalchemy
#!pip install geoalchemy

# Downloading OSM data from Geofabrik
import wget
url ="http://download.geofabrik.de/europe/germany/bremen-latest.osm.pbf"
bremen_data = wget.download(url)

# Script handling the the data 
import osmium as osm
import pandas as pd

class OSMHandler(osm.SimpleHandler):
    def __init__(self):
        osm.SimpleHandler.__init__(self)
        self.osm_data = []

    def tag_inventory(self, elem, elem_type):
        for tag in elem.tags:
            if elem_type == 'relation':
                members = [(m.type, m.ref, m.role) for m in elem.members]
            else:
                members = 'None'

            self.osm_data.append([elem_type, 
                               elem.id, 
                               elem.version,
                               elem.visible,
                               pd.Timestamp(elem.timestamp),
                               elem.changeset,
                               len(elem.tags),
                               tag.k,
                               tag.v, 
                               members
                               ])


    def node(self, n):
        self.tag_inventory(n, "node")

    def way(self, w):
        self.tag_inventory(w, "way")

    def relation(self, r):
        self.tag_inventory(r, "relation")
    

osmhandler = OSMHandler()
osmhandler.apply_file(bremen_data)
data = osmhandler.osm_data
data

# Putting data in a dataframe
import pandas as pd
df = pd.DataFrame(data, columns= ['type', 'id', 'version', 'visible', 'tstamp', 'changeset_id',
                                       'total_tags', 'tag_k', 'tag_v', 'member'])
df.head()

# Converting timestamp to timestamp without time zone
#import datetime as dt
df ['tstamp'] = df.tstamp.dt.tz_localize(None)
df.head(2)

len(df)

# Filtering based on condition [power] on the key column
power_df = df[df['tag_k'] == 'power']
power_df.head()

# Filtering based on condition [highways] on the key column
highway_df = df[df['tag_k'] == 'highway']
highway_df.head()

# Filtering based on condition [restauants] on the key column
school_df = df[df['tag_k'] == 'school']
school_df.head()

# selecting rows based on condition relations on highway data
relations_df = highway_df[highway_df['type'] == 'relation']
relations_df.head()

# selecting rows based on condition nodes highway data
nodes_df = highway_df[highway_df['type'] == 'node']
nodes_df.head()

len(nodes_df)

# selecting rows based on condition ways highway data
ways_df = highway_df[highway_df['type'] == 'way']
ways_df.head()

len(ways_df)

# Getting all nodes id to ways id then joining the ways id dataframe to the ways dataframe
class OSMHandler(osm.SimpleHandler):
    def __init__(self):
        osm.SimpleHandler.__init__(self)
        self.osm_data = dict()

    def tag_inventory(self, elem, elem_type):
        for tag in elem.tags:
            self.osm_data[int(elem.id)] = dict()
#             self.osm_data[int(elem.id)]['is_closed'] = str(elem.is_closed)
            self.osm_data[int(elem.id)]['nodes'] = [str(n) for n in elem.nodes]

    def way(self, w):
        self.tag_inventory(w, "way")

osmhandler = OSMHandler()
osmhandler.apply_file(bremen_data)
ways = osmhandler.osm_data

ways

ways1=[]
for item, i in ways.items():
    for n in i:
        ways1.append([item, i[n]])
ways_id = pd.DataFrame(ways1, columns=['id', 'node_id'])
ways_id.head()


len(ways_id)
ways_data = ways_id.merge(ways_df, how='inner', on='id')
ways_data.head(2)

len(ways_data)
# Getting all coordinates to nodes then joining the nodes coordinates dataframe to the nodes dataframe

# Getting coordinates
import osmium

class CounterHandler(osmium.SimpleHandler):
    def __init__(self):
        osmium.SimpleHandler.__init__(self)
        self.osm_data = dict()

    def node(self, n):
        self.osm_data[int(n.id)] = [n.location.lat, n.location.lon]
      
h = CounterHandler()
h.apply_file(bremen_data)
nodes = h.osm_data

nodes
# Putting data into a list
nodes1=[]
for item in nodes:
    nodes1.append([item, nodes[item]])
nodes1

df6 = pd.DataFrame(nodes1, columns=['id', 'geom'])
df6.head()
len(df6)
df15 = df6.merge(nodes_df, how='inner', on='id')
df15.head()
# Separating longititute and latitudes
df20 = pd.concat([df6['geom'].apply(pd.Series), df6.drop('geom', axis = 1)], axis = 1)
df20.head()

#Renaming Columns and joining the two tables 
df20 = df20.rename(columns={0 : "lat", 1 : "lon"})
df15 = df20.merge(nodes_df, how='inner', on='id')
df15.head()
df20.info()

# Getting a 2 point geometry
from geopandas import GeoDataFrame
from shapely.geometry import Point
geometry = [Point(xy) for xy in zip(df15.lon, df15.lat)]
df21 = df15.drop(['lon', 'lat'], axis=1)
nodes_data = GeoDataFrame(df21, crs="EPSG:4326", geometry=geometry)
nodes_data.head()

# Pushing data to Postgres Database
# exporting table to postgress for new data
from sqlalchemy import create_engine
engine = create_engine('postgresql://username:password@hostname:port_number/database_name')
nodes_data.to_sql('table_name', engine, index=False)

# updating an existing table
from sqlalchemy import create_engine
engine = create_engine('postgresql://username:password@hostname:port_number/database_name')
relations_new.to_sql('relations', engine, if_exists='append', index=False)
