# remove hash below to install package
#!pip3 install owslib==0.25.0 fiona==1.8.21 geopandas==0.10.2 requests==2.28.0 folium==0.12.1

from owslib.wfs import WebFeatureService
import geopandas
import folium
import io
import zipfile
import pandas as pd
import os
from urllib.request import urlretrieve

# User credential to connect with API
WFS_USERNAME = 'ilcag'
WFS_PASSWORD= 'NCpx3cztUjshvRDb'
WFS_URL='https://adp.aurin.org.au/geoserver/wfs'

# Connect with AURIN API
adp_client = None
while adp_client is None:
    try:
        adp_client = WebFeatureService(url=WFS_URL,username=WFS_USERNAME, password=WFS_PASSWORD, version='2.0.0')
    except:
        pass

def download_aurin_df(type_name, file_name):
    """
        This function downloads the dataset from AURIN API
        
        type_name: dataset identifier from the website
        file_name: output file name 
    """

    output_dir = '../data/abs'
    
    # check if directed folder exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    df_result = None
    while df_result is None:
        try:
            response = adp_client.getfeature(typename=type_name)
            out = open(f'{output_dir}/{file_name}.gml', 'wb')
            out.write(response.read())
            out.close()
            print(f'Completed download {file_name}')
            df_result = 1
        except:
            pass
    return df_result

def download_url(url, file_name, file_extension):
    '''
        This function downloads data from the specified url.

        url: url of specified website
        filename: output file name
    '''
    output_dir = '../data/abs'
    
    # check if directed folder exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    output_dir = f"{output_dir}/{file_name}{file_extension}"
    urlretrieve(url, output_dir)
    print(f"Completed download {file_name}")



# download the selected ABS datasets from AURIN
download_aurin_df('datasource-AU_Govt_ABS-UoM_AURIN_DB_GeoLevel:sa2_2016_aust', 'sa2_boundaries')
download_aurin_df('datasource-AU_Govt_ABS-UoM_AURIN_DB_GeoLevel:poa_2016_aust', 'poa_boundaries')
download_aurin_df('datasource-AU_Govt_ABS-UoM_AURIN_DB_3:abs_personal_income_total_income_distribution_sa2_2017_18', 'sa2_income')
download_aurin_df('datasource-AU_Govt_ABS-UoM_AURIN_DB_3:abs_regional_population_age_sex_sa2_2019', 'sa2_age')

# download from ABS data from download link
download_url('https://www.abs.gov.au/AUSSTATS/subscriber.nsf/log?openagent&1270055006_CG_POSTCODE_2011_SA2_2011.zip&1270.0.55.006&Data%20Cubes&70A3CE8A2E6F9A6BCA257A29001979B2&0&July%202011&27.06.2012&Latest', 'poa_sa2_lookup', '.zip')




