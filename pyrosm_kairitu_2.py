# pip install pyrosm

# Extracting data from OSM

from pyrosm import OSM
from pyrosm import get_data
fp = get_data("Croatia")

# Initialize the OSM parser object with test data from Helsinki
osm = OSM(fp)

# Test reading all transit related data (bus, trains, trams, metro etc.)
# Exclude nodes (not keeping stops, etc.)
power = ["switchgear", "switch", "generator", 'tower']

power_data = osm.get_data_by_custom_criteria(
  custom_filter={
    'power': power
  }
  # Keep data matching the criteria above
  filter_type="keep",
  # Do not keep nodes (point data)    
  keep_nodes=True, 
  keep_ways=True, 
  keep_relations=True
)

# selecting rows based on condition for nodes
node_df = power_data[power_data['osm_type'] == 'node']
       
# selecting columns to for nodes
node_new = node_df[['id', 'version', 'timestamp', 'changeset', 'tags', 'geometry']]

# selecting rows based on condition ways
ways_df = power_data[power_data['osm_type'] == 'way']
# selecting columns to for ways
ways_new = ways_df[['id', 'version', 'timestamp', 'changeset', 'tags', 'geometry']]

# selecting rows based on condition relations
relations_df = power_data[power_data['osm_type'] == 'relation']
            
# selecting columns to for ways
relations_new = relations_df[['id', 'version', 'timestamp', 'changeset', 'tags']]

# data to be pushed to Postgres Database
node_df.head()

# data to be pushed to Postgres Database
ways_df.head()

# data to be pushed to Postgres Database
relations_df.head()

# exporting table to postgress for new data
from sqlalchemy import create_engine
engine = create_engine('postgresql://username:password@hostname:port_number/database_name')
node_new.to_sql('table_name', engine, index=False)

# updating an existing table
from sqlalchemy import create_engine
engine = create_engine('postgresql://username:password@hostname:port_number/database_name')
relations_new.to_sql('relations', engine, if_exists='append', index=False)
