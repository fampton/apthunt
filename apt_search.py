#!/usr/bin/env python

import requests

import pony.orm as pny

from datetime import datetime as dt

database = pny.Database('sqlite', 'pdxapartments.sqlite', create_db=True)

class Apartment(database.Entity):
  ask = pny.Required(int)
  bedrooms = pny.Required(int)
  imageurls = pny.Required(str)
  title = pny.Required(str)
  lat = pny.Required(float)
  lon = pny.Required(float)
  # craigslist listing ID
  clid = pny.Required(int, size=64)
  postdate = pny.Required(dt)
  clurl = pny.Required(str)

class Cluster(database.Entity):
  geocluster = pny.Required(int, size=64)
  lat = pny.Required(float)
  lon = pny.Required(float)
  clid = pny.Required(str)
  clurl = pny.Required(str)

database.generate_mapping(create_tables=True)
pdxclurl = 'https://portland.craigslist.org/jsonsearch/apa/mlt'
southeast = (45.501790,-122.622177)
northwest = (45.525216,-122.655179)

def params(minp, maxp):
  return {'min_price': minp, 'max_price': maxp, 'min_bedrooms': 1, 'max_bedrooms': 2, 'map': 1, 'availabilityMode': 0}

def AptSearch(min,max):
  results = requests.get(pdxclurl,params=params(min,max))
  if results.json():
    return results.json()[0]
  else:
    return 'No json returned'

@pny.db_session
def main():
  import sys
  min = sys.argv[1]
  max = sys.argv[2]
  # check if entry in returned json is a listing or a cluster of listings by looking at its keys 'Ask' denotes apartment ask price
  apartments = [i for i in AptSearch(min,max) if 'Ask' in i.keys()]
  se_apt = [apartment for apartment in apartments if ((northwest[0] > apartment['Latitude'] > southeast[0]) and (northwest[1] < apartment['Longitude'] < southeast[1]))]
  clusters = [i for i in AptSearch(min,max) if 'Ask' not in i.keys()]
  se_clusters = [cluster for cluster in clusters if ((northwest[0] > cluster['Latitude'] > southeast[0]) and (northwest[1] < cluster['Longitude'] < southeast[1]))]
  for apartment in se_apt:
    clid = apartment['PostingID']
    if not database.exists("select * from Apartment where clid = $clid"):
      apartment = Apartment(
        ask=apartment['Ask'],
        bedrooms=apartment['Bedrooms'],
        title=apartment['PostingTitle'],
        lat=apartment['Latitude'],
        lon=apartment['Longitude'],
        clid=apartment['PostingID'],
        postdate=dt.fromtimestamp(apartment['PostedDate']),
        clurl=apartment['PostingURL'],
        # Can store image urls and later add feature that looks for duplicate postings based on either text or image
        imageurls='testurl',
        )
  for cluster in se_clusters:
    if not database.exists("select * from Cluster where clid = $clid"):
      cluster = Cluster(
        geocluster = cluster['GeoCluster'],
        lat = cluster['Latitude'],
        lon = cluster['Longitude'],
        clid = cluster['PostingID'],
        clurl = cluster['url']
        )
  # I dont remember why this is commented out and when its needed.
  #pny.commit()

if __name__ == '__main__':
    main()
