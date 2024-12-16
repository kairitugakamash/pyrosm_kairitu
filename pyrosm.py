# maldives
from pyrosm import OSM
from pyrosm import get_data

path = "path to download data"

fp = get_data("maldives", 
              directory=path,
              update=True)
# print(fp)

# Initialize the OSM parser object
osm = OSM(fp)

natural = osm.get_natural()
natural.plot(column='natural', legend=True, figsize=(10,6))

# Read all boundaries using the default settings
boundaries = osm.get_boundaries()
boundaries.plot(facecolor="none", edgecolor="blue")
