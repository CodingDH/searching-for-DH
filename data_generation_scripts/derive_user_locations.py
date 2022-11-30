import argparse
from geopy.geocoders import Nominatim 
from geopy.extra.rate_limiter import RateLimiter
import os
import pandas as pd
from tqdm import tqdm
tqdm.pandas()


parser = argparse.ArgumentParser(description="Function Variables")
parser.add_argument("inputFile", type=str)
parser.add_argument("email", type=str)
parser.add_argument("locationField", type=str)
args = parser.parse_args()

email = args.email
inputFile = args.inputFile
locationField = args.locationField

data_df = pd.read_csv(inputFile)

located_users = data_df[data_df[locationField].notnull()]

places = pd.DataFrame(located_users[locationField].unique(), columns=['location'])

geolocator = Nominatim(user_agent=email)
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

places['full_location'] = places['location'].progress_apply(geocode)
places['latitude'] = places['full_location'].progress_apply(lambda loc: loc.latitude if loc else None)
places['longitude'] = places['full_location'].progress_apply(lambda loc: loc.longitude if loc else None)

places.to_csv(os.path.join('../data/derived_files', "geolocated_locations.csv"), index=False)


