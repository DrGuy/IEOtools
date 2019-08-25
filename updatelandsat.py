#!/usr/bin/env python3
# Guy Serbin, EOanalytics Ltd.
# Talent Garden Dublin, Claremont Ave. Glasnevin, Dublin 11, Ireland
# email: guyserbin <at> eoanalytics <dot> ie

# version 1.3

# This script will create and update a geopackage layer of all available Landsat TM/ETM+/OLI-TIRS scenes, including available metadata
# Changes:
# 23 May 2018: XML functionality deprecated in favor of JSON queries, as the former is no longer available or efficient
# 25 March 2019: This script will now read configuration data from ieo.ini
# 14 August 2019: This now creates and updates a layer within a geopackage, and will migrate data from an old shapefile to a new one

import os, sys, urllib.error, datetime, shutil, glob, argparse, json, getpass, requests, math #, ieo
from osgeo import ogr, osr
#import xml.etree.ElementTree as ET
from PIL import Image

try: # This is included as the module may not properly install in Anaconda.
    import ieo
except:
    print('Error: IEO failed to load. Please input the location of the directory containing the IEO installation files.')
    ieodir = input('IEO installation path: ')
    if os.path.isfile(os.path.join(ieodir, 'ieo.py')):
        sys.path.append(ieodir)
        import ieo
    else:
        print('Error: that is not a valid path for the IEO module. Exiting.')
        sys.exit()

if sys.version_info[0] == 2:
    from urllib import urlretrieve
    from urllib2 import urlopen, URLError
else:
    from urllib.request import urlopen, urlretrieve
    from urllib.error import URLError

global pathrows, errorsfound

config = ieo.config

pathrowvals = config['Landsat']['pathrowvals'] # this is a comma-delimited string containing multiples of four values: start path, end path, start row, end row. It is designed to query rectangular path/row combinations, in order to avoid scenes that don't touch landmasses or are not of interest.
useWRS2 = config['Landsat']['useWRS2'] # Setting this parameter to "Yes" in updateshp.ini will query WRS-2 Path/ Row field values from ieo.WRS2, and may result in a great increase in the number of queries to USGS servers

parser = argparse.ArgumentParser('This script imports LEDAPS-processed scenes into the local library. It stacks images and converts them to the locally defined projection in IEO, and adds ENVI metadata.')
parser.add_argument('-u','--username', type = str, default = None, help = 'USGS/EROS Registration System (ERS) username.')
parser.add_argument('-p', '--password', type = str, default = None, help = 'USGS/EROS Registration System (ERS) password.')
parser.add_argument('-c', '--catalogID', type = str, default = 'EE', help = 'USGS/EROS Catalog ID (default = "EE").')
parser.add_argument('-v', '--version', type = str, default = "1.4.0", help = 'JSON version, default = 1.4.0.')
parser.add_argument('--startdate', type = str, default = "1982-07-16", help = 'Start date for query in YYYY-MM-DD format. (Default = 1982-07-16, e.g., Landsat 4 launch date).')
parser.add_argument('--enddate', type = str, default = None, help = "End date for query in YYYY-MM-DD format. (Default = today's date).")
parser.add_argument('-m', '--MBR', type = str, default = None, help = 'Minimum Bounding Rectangle (MBR) coordinates in decimal degrees in the following format (comma delimited, no spaces): lower left latitude, lower left longitude, upper right latitude, upper right longitude. If not supplied, these will be determined from WRS-2 Paths and Rows in updateshp.ini.')
parser.add_argument('-b', '--baseURL', type = str, default = 'https://earthexplorer.usgs.gov/inventory/json/v/', help = 'Base URL to use excluding JSON version (Default = "https://earthexplorer.usgs.gov/inventory/json/v/").')
parser.add_argument('--maxResults', type = int, default = 50000, help = 'Maximum number of results to return (1 - 50000, default = 50000).')
parser.add_argument('--overwrite', type = bool, default = False, help = 'Overwrite existing files.')
parser.add_argument('--thumbnails', type = bool, default = True, help = 'Download thumbnails (default = True).')
parser.add_argument('--savequeries', action = 'store_true', help = 'Save queries.')
parser.add_argument('--usesaved', action = 'store_true', help = 'Use any saved queries on disk, rather than online.')
parser.add_argument('--migrate', type = bool, default = False, help = 'Force migration of Landsat shapefile data to catalog geopackage.')
parser.add_argument('--verbose', type = bool, default = False, help = 'Display more messages during migration..')
parser.add_argument('-t', '--tiledir', type = str, default = os.path.dirname(ieo.srdir), help = 'Directory path for tile subdirectories.')

args = parser.parse_args()

if not (args.username and args.password):
    if not args.username:
        args.username = input('USGS/ERS username: ')
    if not args.password:
        args.password = getpass.getpass('USGS/ERS password: ')

subpathrow = []

ingestdir = os.path.join(ieo.ingestdir, 'Metadata')
dirname = os.path.join(ieo.catdir, 'Landsat')
logdir = ieo.logdir
jpgdir = os.path.join(ieo.catdir, 'Landsat', 'Thumbnails')
itmdir = ieo.srdir
shapefile = ieo.landsatshp # This is a layer in a geopackage, not a shapefile any longer
layername = ieo.landsatshp # os.path.basename(shapefile)[:-4] # assumes a shapefile ending in '.shp'
shapefilepath = os.path.join(ieo.catdir, 'Landsat', '{}.shp'.format(shapefile))
errorlist = []
scenelist = []
if not args.enddate:
    today = datetime.datetime.today()
    args.enddate = today.strftime('%Y-%m-%d')

errorfile = os.path.join(logdir, 'Landsat_inventory_download_errors.csv')
errorsfound = False

pathrowstrs = [] # list of strings containing WRS-2 Path/ Row combinations
paths = [] # list containing WRS-2 Paths
rows = [] # List containing WRS-2 Rows

if useWRS2.lower() == 'yes':
    print('Getting WRS-2 Path/Row combinations from geopackage: {}'.format(ieo.WRS2))
    driver = ogr.GetDriverByName("GPKG")
    print('WRS-2 = {}'.format(ieo.WRS2))
    ds = driver.Open(ieo.ieogpkg, 0)
    layer = ds.GetLayer(ieo.WRS2)
    for feature in layer:
        path = feature.GetField('PATH')
        if not path in paths:
            paths.append(path)
        row = feature.GetField('ROW')
        if not row in rows:
            rows.append(row)
        pathrowstrs.append('{:03d}{:03d}'.format(path, row))
    ds = None
else:
    print('Using WRS-2 Path/Row combinations from INI file.')
    pathrowvals = pathrowvals.split(',')
    iterations = int(len(pathrowvals) / 4)
    for i in range(iterations):
        for j in range(int(pathrowvals[i * 4]), int(pathrowvals[i * 4 + 1]) + 1):
            if not j in paths:
                paths.append(j)
            for k in range(int(pathrowvals[i * 4 + 2]), int(pathrowvals[i * 4 + 3]) + 1):
                pathrowstrs.append('{:03d}{:03d}'.format(j, k))
                if not k in rows:
                    rows.append(k)

## JSON functions

def getapiKey():
    # This function gets the apiKey used for all queries to the USGS/EROS servers
    URL = '{}{}/login'.format(args.baseURL, args.version)
    print('Logging in to: {}'.format(URL))
    data = json.dumps({'username': args.username, 'password': args.password, 'catalog_ID': args.catalogID})
    response = requests.post(URL, data = {'jsonRequest':data}) #
    json_data = json.loads(response.text)
    apiKey = json_data['data']
    return apiKey

def getMBR():
    # This creates the Minimum Bounding Rectangle (MBR) for JSON queries
    URL = '{}{}/grid2ll'.format(args.baseURL, args.version)
    prs = [[min(paths), min(rows)], [min(paths), max(rows)], [max(paths), max(rows)], [max(paths), min(rows)]]
    Xcoords = []
    Ycoords = []
    for pr in prs:
        print('Requesting coordinates for WRS-2 Path {} Row {}.'.format(pr[0], pr[1]))
        jsonRequest = json.dumps({"gridType" : "WRS2", "responseShape" : "point", "path" : str(pr[0]), "row" : str(pr[1])}).replace(' ','')
        requestURL = '{}?jsonRequest={}'.format(URL, jsonRequest)
        response = requests.post(requestURL) # URL, data = {'jsonRequest': jsonRequest}
        json_data = json.loads(response.text)
        Xcoords.append(float(json_data["data"]["coordinates"][0]["longitude"]))
        Ycoords.append(float(json_data["data"]["coordinates"][0]["latitude"]))
    return [min(Ycoords), min(Xcoords), max(Ycoords), max(Xcoords)]

def scenesearch(apiKey, scenelist, updatemissing, badgeom, lastmodifiedDate):
    # This searches the USGS archive for scene metadata, and checks it against local metadata. New scenes will be queried for metadata.
    RequestURL = '{}{}/search'.format(args.baseURL, args.version)
    QueryURL = '{}{}/metadata'.format(args.baseURL, args.version)
    datasetNames = {'LANDSAT_8_C1' : '2013-02-11', 'LANDSAT_ETM_C1' : '1999-04-15', 'LANDSAT_TM_C1' : '1982-07-16'}
    scenedict = {}
#    js = {'LL': 0, 'UL': 1, 'UR': 2, 'LR': 3}
    for datasetName in datasetNames.keys():
        print('Querying collection: {}'.format(datasetName))
        if lastmodifiedDate and not (len(updatemissing) > 0 or len(badgeom) > 0):
            startdate = lastmodifiedDate
        else:
            startdate = args.startdate
        if '/' in startdate:
            startdate = startdate.replace('/', '-')
        datetuple = datetime.datetime.strptime(startdate, '%Y-%m-%d')
        sensorstarttuple = datetime.datetime.strptime(datasetNames[datasetName], '%Y-%m-%d') # restrict searches to times from which sensor was in orbit
        if datetuple < sensorstarttuple:
            datetuple = sensorstarttuple
        enddatetuple = datetime.datetime.strptime(args.enddate, '%Y-%m-%d')
        if datasetName == 'LANDSAT_TM_C1':
            l5enddatetuple = datetime.datetime.strptime('2013-06-05', '%Y-%m-%d') # end of Landsat 5 mission
            if l5enddatetuple < enddatetuple:
                enddatetuple = l5enddatetuple
        while datetuple < enddatetuple:
            edatetuple = datetuple + datetime.timedelta(days = 365) # iterate by year
            if edatetuple > enddatetuple:
                edatetuple = enddatetuple
            startdate = datetuple.strftime('%Y-%m-%d')
            enddate = edatetuple.strftime('%Y-%m-%d')
            print('Now searching for scene data from collection {} from {} through {}.'.format(datasetName, startdate, enddate))
            searchparams = json.dumps({"apiKey": apiKey,
                            "datasetName": datasetName,
                            "spatialFilter":{"filterType": "mbr",
                                             "lowerLeft":{"latitude": args.MBR[0],
                                                          "longitude": args.MBR[1]},
                                             "upperRight":{"latitude": args.MBR[2],
                                                           "longitude": args.MBR[3]}},
                            "temporalFilter":{"startDate": startdate,
                                              "endDate": enddate},
                            "includeUnknownCloudCover":False,
                            "maxCloudCover": 100,
                            "maxResults": args.maxResults,
                            "sortOrder": "ASC"})
            response = requests.post(RequestURL, data = {'jsonRequest': searchparams})
            json_data = json.loads(response.text)
            querylist = []
            for i in range(len(json_data['data']['results'])):
                sceneID = json_data['data']['results'][i]['entityId']
                if sceneID[3:9] in pathrowstrs and (not sceneID in scenelist or sceneID in updatemissing or sceneID in badgeom):
                    querylist.append(sceneID)
                    scenedict[sceneID] = {'Landsat Product Identifier': json_data['data']['results'][i]["displayId"],
                             "browseUrl": json_data['data']['results'][i]["browseUrl"],
                             "dataAccessUrl": json_data['data']['results'][i]["dataAccessUrl"],
                             "downloadUrl": json_data['data']['results'][i]["downloadUrl"],
                             "metadataUrl": json_data['data']['results'][i]["metadataUrl"],
                             "fgdcMetadataUrl": json_data['data']['results'][i]["fgdcMetadataUrl"],
                             'modifiedDate': datetime.datetime.strptime(json_data['data']['results'][i]["modifiedDate"], '%Y-%m-%d'),
                             "orderUrl": json_data['data']['results'][i]["orderUrl"],
                             'Dataset Identifier': datasetName,
                             'updatemodifiedDate': False,
                             'updategeom': False}
    
            if len(querylist) > 0:
                print('{} new scenes have been found or require updating, querying metadata.'.format(len(querylist)))
                iterations = math.ceil(len(querylist) / 100) # break up queries into blocks of 100 or less scenes
                total = 0
    #            iterations = 1 # temporary limitation
                for iteration in range(iterations):
                    startval = iteration * 100
                    if iteration * 100 > len(querylist):
                        endval = len(querylist) - startval - 1
                    else:
                        endval = startval + 99
                    total += endval + 1
                    print('Now querying {} scenes, query {}/{}.'.format((endval - startval + 1), iteration + 1, iterations))
                    querystr = ''
    
                    for sceneID in querylist[startval: endval]:
                        querystr += ',{}'.format(sceneID)
                    querystr = querystr[1:]
                    queryparams = json.dumps({"apiKey":apiKey,
                                "datasetName":datasetName,
                                'entityIds': querystr})
                    query = requests.post(QueryURL, data = {'jsonRequest':queryparams})
    #                if endval == 99:
                    
                    if args.savequeries:
                        now = datetime.datetime.now()
                        outfile = os.path.join(ieo.ingestdir, 'query_{}_{}.txt'.format(datasetName, now.strftime('%Y%m%d-%H%M%S')))
                        with open(outfile, 'w') as output:
                            output.write(query.text)
                    querydict = json.loads(query.text)
                    if len(querydict['data']) > 0:
                        
                        for item in querydict['data']:
                            if len(item['metadataFields']) > 0:
                                if item['metadataFields'][1]['fieldName'] == 'Landsat Scene Identifier':
                                    sceneID = item['metadataFields'][1]['value']
                                else:
                                    for subitem in item['metadataFields']:
                                        if subitem['fieldName']  == 'Landsat Scene Identifier':
                                            sceneID = subitem['value']
                                            break
                                for subitem in item['metadataFields']:
                                    fieldname = subitem['fieldName'].rstrip().lstrip().replace('L-1', 'L1')
                                    if fieldname in queryfieldnames and not fieldname in scenedict[sceneID].keys() and fieldname != 'Landsat Scene Identifier':
                                        value = subitem['value']
                                        if value:
                                            i = queryfieldnames.index(fieldname)
                                            if fieldvaluelist[i][3] == ogr.OFTDate or fieldname.endswith('Date'):
                                                if 'Time' in fieldname:
                                                    value = datetime.datetime.strptime(value[:-1], '%Y:%j:%H:%M:%S.%f')
                                                elif '/' in value:
                                                    value = datetime.datetime.strptime(value, '%Y/%m/%d')
                                                else:
                                                    value = datetime.datetime.strptime(value, '%Y-%m-%d')
                                            elif fieldvaluelist[i][3] == ogr.OFTReal:
                                                value = float(value)
                                            elif fieldvaluelist[i][3] == ogr.OFTInteger:
                                                try:
                                                    value = int(value)
                                                except:
                                                    print('Error: fieldname {} has a value of {}.'.format(fieldname, value))
                                                    sys.exit()
                                            elif fieldname == 'browseUrl':
                                                if value:
                                                    if value.lower() != 'null':
                                                        scenedict[sceneID]['browse'] = 'Y'
                                                    else:
                                                        scenedict[sceneID]['browse'] = 'N'
                                            elif fieldname == 'Data Type Level-1':
                                                j = value.rfind('_') + 1
                                                value = value[j:]
                                            scenedict[sceneID][fieldname] = value
                                if sceneID in badgeom or sceneID in updatemissing:
                                    scenedict[sceneID]['updatemodifiedDate'] = True 
                                else: 
                                    scenedict[sceneID]['updatemodifiedDate'] = False 
                                if sceneID in badgeom:
                                    scenedict[sceneID]['updategeom'] = True
                                else: 
                                    scenedict[sceneID]['updategeom'] = False
                                scenedict[sceneID]['coords'] = item['spatialFootprint']['coordinates'][0]
                                scenedict[sceneID]['modifiedDate'] = item['modifiedDate']
    
                        if not 'Spacecraft Identifier' in scenedict[sceneID].keys():
                            scenedict[sceneID]['Spacecraft Identifier'] = 'LANDSAT_{}'.format(sceneID[2:3])
            datetuple = edatetuple + datetime.timedelta(days = 1)
    return scenedict

def findlocalfiles(sceneID, fielddict, scenedict):
    tilebase = '{}_{}'.format(sceneID[:3], sceneID[9:16])
    for fieldname in fielddict:
        tilelist = glob.glob(os.path.join(fielddict[fieldname]['dirname'], '{}*.dat'.format(tilebase)))
        tiles = []
        tilestr = None
        if len(tilelist) > 0:
            for f in tilelist:
                parentrasters = ieo.readenvihdr(f.replace('.dat', '.hdr'))['parent rasters']
                if sceneID in parentrasters:
                    basename = os.path.basename(f)
                    i = basename.rfind('_') + 1
                    j = f.find('.')
                    tiles.append(basename[i:j])
            if len(tiles) > 0:
                tilestr = tiles[0]
                if len(tiles) > 1:
                    for i in range(1, len(tiles)):
                        tilestr += ',{}'.format(tiles[i])
                scenedict[fieldname] = tilestr
                if fieldname == 'Pixel_QA_tiles':
                    scenedict['MaskType'] = 'Pixel_QA'
                elif fieldname == 'Fmask_tiles':
                    scenedict['MaskType'] = 'FMask'
    
#    srstr = feature.GetField('Surface_Reflectance_tiles')
#    if isinstance(srstr, str):
#        srlist = srstr.split(',')
#        tilebase = feature.GetField('Tile_filename_base')
#        itm = os.path.join(itmdir, '{}_{}.dat'.format(tilebase, srlist[0]))
#    else:
#        itm = ''
#    if not os.path.isfile(itm): # Populate 'SR_path' field if surface reflectance data are present in library
#        
#        if len(itmlist) > 0:
#            itm = itmlist[0]
#        else:
#            itm = os.path.join(itmdir, '{}_ref_{}.dat'.format(scenedict[sceneID]['Landsat Product Identifier'], ieo.projacronym))
#            if not os.path.isfile(itm):
#                itm = None
#    if itm:
#        scenedict['Surface_Reflectance_tiles'] = srstr
#        for key in fielddict.keys():
#            value = feature.GetField(key)
#            if isinstance(value, str):
#                scenedict[key] = value
#        if isinstance(feature.GetField('Pixel_QA_tiles'), str):
#            scenedict['MaskType'] = 'Pixel_QA'
#        elif isinstance(feature.GetField('Fmask_tiles'), str):
#            scenedict['MaskType'] = 'FMask'
    return scenedict

## Migration functions
    
def migrate(layer, shapefilepath, fieldvaluelist, *args, **kwargs):
    # added on 14 August 2019
    # This will migrate features from a shapefile to a geopackage if they have reasonable geometries.
    tiledir = kwargs.get('tiledir', os.path.dirname(ieo.srdir))
    verbose = kwargs.get('verbose', verbose)
    print('Migrating data from shapefile to geopackage.')
    fnamelist = []
    tilesearchdict = {'SR_path' : os.path.join(tiledir, os.path.basename(ieo.srdir)), 
                      'BT_path' : os.path.join(tiledir, os.path.basename(ieo.btdir)), 
                      'Fmask_path' : os.path.join(tiledir, os.path.basename(ieo.fmaskdir)), 
                      'PixQA_path' : os.path.join(tiledir, os.path.basename(ieo.pixelqadir)), 
                      'NDVI_path' : os.path.join(tiledir, os.path.basename(ieo.ndvidir)), 
                      'EVI_path' : os.path.join(tiledir, os.path.basename(ieo.evidir))}
    fieldvaluedict = {}
    for item in fieldvaluelist:
        fieldvaluedict[item[0]] = item[1]
    fieldvaluedict['MaskType'] = 'Scene_mask_type'
    fieldvaluedict['Thumb_JPG'] = 'Thumbnail_filename'
    fieldvaluedict['SR_path'] = 'Surface_reflectance_tiles'
    fieldvaluedict['BT_path'] = 'Brightness_temperature_tiles'
    fieldvaluedict['Fmask_path'] = 'CFmask_tiles'
    fieldvaluedict['PixQA_path'] = 'Pixel_QA_tiles'
    fieldvaluedict['NDVI_path'] = 'NDVI_tiles'
    fieldvaluedict['EVI_path'] = 'EVI_tiles'
    fieldvaluedict['tilebase'] = 'Tile_filename_base'                
    shpdriver = ogr.GetDriverByName("ESRI Shapefile")
    ds = shpdriver.Open(shapefilepath, 0)
    shplayer = ds.GetLayer()
    shplayerDefinition = shplayer.GetLayerDefn()
#    layerDefinition = layer.GetLayerDefn()
    featureCount = layer.GetFeatureCount()
    sceneids = []
    if featureCount > 0:
        for feat in layer:
            sceneids.append(feat.GetField('sceneID'))
    layer.ResetReading()
    for i in range(shplayerDefinition.GetFieldCount()):
        fnamelist.append(shplayerDefinition.GetFieldDefn(i).GetName())
    for feature in shplayer:
        sceneid = feature.GetField('sceneID')
        if (not ieo.checkscenegeometry(feature, verbose = verbose)) and (sceneid in sceneids):
            print('Migrating feature for SceneID {} and associated metadata.'.format(sceneid))
            outfeature = ogr.Feature(layer.GetLayerDefn())
            tilebase = feature.GetField('tilebase')
            for field in fnamelist:
                if field in tilesearchdict.keys():
                    tilestr = ''
                    filelist = glob.glob(os.path.join(tilesearchdict[field], '{}_*.dat'.format(tilebase)))
                    if len(filelist) > 0:
                        for f in filelist:
                            basename = os.path.basename(f)
                            j = basename.find('.dat')
                            tilestr += ',{}'.format(basename[12:j])
                        outfeature.SetField(fieldvaluedict[field], tilestr[1:])
                elif field in fieldvaluedict.keys():
                    if field == 'Thumbnail_filename':
                        value = feature.GetField(field)
                        if not os.path.isfile(value):
                            value = os.path.join(jpgdir, feature.GetField('LandsatPID'))
                            if not os.path.isfile(value):
                                value = os.path.join(jpgdir, feature.GetField('sceneID'))
                        if os.path.isfile(value):
                            outfeature.SetField(fieldvaluedict[field], os.path.basename(value)) # from now on, only base filenames will be included
                    else:
                        outfeature.SetField(fieldvaluedict[field], feature.GetField(field))
            geom = feature.GetGeometryRef()
            outfeature.SetGeometry(geom)
            layer.SetFeature(outfeature)
            outfeature.Destroy()
    ds = None
    print('Feature migration complete.')
    return layer


## Old XML functions, deprecated

def dlxmls(startdate, enddate, xmls, ingestdir, *args, **kwargs): # This downloads queried XML files
    global errorsfound
    tries = 1
    downloaded = False
    for x, p in zip(xmls, pathrows):

        print('Downloading {} to: {}'.format(x, ingestdir))
        xml = os.path.join(ingestdir, x)
        if os.access(xml, os.F_OK):
            print('Backing up current xml file.')
            shutil.move(xml, '{}.{}.bak'.format(xml, today.strftime('%Y%m%d-%H%M%S')))
        urlname = 'http://earthexplorer.usgs.gov/EE/InventoryStream/pathrow?start_path={}&end_path={}&start_row={}&end_row={}&sensor_name=LANDSAT_COMBINED_C1&start_date={}&end_date={}'.format(p[0], p[1], p[2], p[3], startdate, enddate) #&cloud_cover = 100&seasonal = False&aoi_entry=path_row&output_type=unknown
        tries = 1
        downloaded = False
        while not downloaded and tries < 6:
            print('Download attempt {} of 5.'.format(tries))
            try:
                urlretrieve(urlname, xml) # filename=xml
                downloaded = True
            except URLError as e:
                print(e.reason)
                ieo.logerror(urlname, e.reason, errorfile = errorfile)
                errorsfound = True
                tries += 1
        if tries == 6:
            ieo.logerror(xml, 'Download error.', errorfile = errorfile)
            print('Download failure: {}'.format(x))
            errorsfound = True
    else:
        return 'Success!'




## Other functions

def dlthumb(url, jpgdir, *args, **kwargs): # This downloads thumbnails from the USGS
    global errorsfound
    basename = os.path.basename(url)
    f = os.path.join(jpgdir, basename)
    tries = 1
    downloaded = False
    print('Downloading {} to {}'.format(basename, jpgdir))
    while not downloaded and tries < 6:
        print('Download attempt {} of 5.'.format(tries))
        try:
            url = urlopen(dlurl)
            urlretrieve(dlurl, filename = f)
            if url.length == os.stat(f).st_size:
                downloaded = True
            else:
                print('Error downloading, retrying.')
                tries += 1
        except urllib.error.URLError as e:
            print(e.reason)
            ieo.logerror(dlurl, e.reason, errorfile = errorfile)
            errorsfound = True
    if tries == 6:
        ieo.logerror(f, 'Download error.', errorfile = errorfile)
        print('Download failure: {}'.format(basename))
        errorsfound = True
    else:
        return 'Success!'

def makeworldfile(jpg, geom): # This attempts to make a worldfile for thumbnails so they can be displayed in a GIS
    img = Image.open(jpg)
    basename = os.path.basename(jpg)
    width, height = img.size
    width = float(width)
    height = float(height)
    minX, maxX, minY, maxY = geom.GetEnvelope()
    if basename[:3] == 'LE7':
        wkt = geom.ExportToWkt()
        start = wkt.find('(') + 2
        end = wkt.find(')')
        vals = wkt[start:end]
        vals = vals.split(',')
        corners = []
        for val in vals:
            val = val.split()
            for v in val:
                corners.append(float(v))
        A = (maxX - corners[0]) / width
        B = (corners[0] - minX) / height
        C = corners[0]
        D = (maxY - corners[3]) / width
        E = (corners[3] - minY) / height
        F = corners[1]
    else:
        A = (maxX - minX) / width
        B = 0.0
        C = minX
        D = (maxY - minY) / height
        E = 0.0
        F = maxY
    jpw = jpg.replace('.jpg', '.jpw')
    if os.access(jpw, os.F_OK):
        bak = jpw.replace('.jpw', '.jpw.{}.bak'.format(today.strftime('%Y%m%d-%H%M%S')))
        shutil.move(jpw, bak)
    with open(jpw, 'w') as file:
        file.write('{}\n-{}\n-{}\n-{}\n{}\n{}\n'.format(A, D, B, E, C, F))
    del img

def reporthook(blocknum, blocksize, totalsize):
    # This makes a progress bar. I did not originally write it, nor do I remember from where I found the code.
    readsofar = blocknum * blocksize
    if totalsize > 0:
        percent = readsofar * 1e2 / totalsize
        s = "\r%5.1f%% %*d / %d" % (
            percent, len(str(totalsize)), readsofar, totalsize)
        sys.stderr.write(s)
        if readsofar >= totalsize: # near the end
            sys.stderr.write("\n")
    else: # total size is unknown
        sys.stderr.write("read %d\n" % (readsofar,))

if args.MBR: # define MBR for scene queries
    args.MBR = args.MBR.split(',')
    if len(args.MBR) != 4:
        ieo.logerror('--MBR', 'Total number of coordinates does not equal four.', errorfile = errorfile)
        print('Error: Improper number of coordinates for --MBR set (must be four). Either remove this option (will use default values) or fix. Exiting.')
        sys.exit()
else:
    args.MBR = getMBR()

# This section borrowed from https://pcjericks.github.io/py-gdalogr-cookbook/projection.html
# Lat/ Lon WGS-84 to local projection transformation
source = osr.SpatialReference() # Lat/Lon WGS-64
source.ImportFromEPSG(4326)

target = ieo.prj

transform = osr.CoordinateTransformation(source, target)

# Create Shapefile
driver = ogr.GetDriverByName("GPKG")

polycoords = ['UL Corner Lat dec', 'UL Corner Long ec', 'UR Corner Lat dec', 'UR Corner Long dec', 'LL Corner Lat dec', 'LL Corner Long dec', 'LR Corner Lat dec', 'LR Corner Long dec']

fieldvaluelist = [
    ['LandsatPID', 'LANDSAT_PRODUCT_ID', 'Landsat Product Identifier', ogr.OFTString, 40],
    ['sceneID', 'sceneID', 'Landsat Scene Identifier', ogr.OFTString, 21],
    ['SensorID', 'SensorID', 'Sensor Identifier', ogr.OFTString, 0],
    ['SatNumber', 'satelliteNumber', 'Spacecraft Identifier', ogr.OFTString, 0],
    ['acqDate', 'acquisitionDate', 'Acquisition Date', ogr.OFTDate, 0],
    ['Updated', 'dateUpdated', 'modifiedDate', ogr.OFTDate, 0],
    ['path', 'path', 'WRS Path', ogr.OFTInteger, 0],
    ['row', 'row', 'WRS Row', ogr.OFTInteger, 0],
    ['CenterLat', 'sceneCenterLatitude', 'Center Latitude dec', ogr.OFTReal, 0],
    ['CenterLong', 'sceneCenterLongitude', 'Center Longitude dec', ogr.OFTReal, 0],
    ['CC', 'cloudCover', 'Cloud Cover Truncated', ogr.OFTInteger, 0],
    ['CCFull', 'cloudCoverFull', 'Scene Cloud Cover', ogr.OFTReal, 0],
    ['CCLand', 'CLOUD_COVER_LAND', 'Land Cloud Cover', ogr.OFTReal, 0],
    ['UL_Q_CCA', 'FULL_UL_QUAD_CCA', 'Cloud Cover Quadrant Upper Left', ogr.OFTReal, 0],
    ['UR_Q_CCA', 'FULL_UR_QUAD_CCA', 'Cloud Cover Quadrant Upper Right', ogr.OFTReal, 0],
    ['LL_Q_CCA', 'FULL_LL_QUAD_CCA', 'Cloud Cover Quadrant Lower Left', ogr.OFTReal, 0],
    ['LR_Q_CCA', 'FULL_LR_QUAD_CCA', 'Cloud Cover Quadrant Lower Right', ogr.OFTReal, 0],
    ['DT_L1', 'DATA_TYPE_L1', 'Data Type Level-1', ogr.OFTString, 0],
    ['DT_L0RP', 'DATA_TYPE_L0RP', 'Data Type Level 0Rp', ogr.OFTString, 0],
    ['L1_AVAIL', 'L1_AVAILABLE', 'L1 Available', ogr.OFTString, 0],
    ['IMAGE_QUAL', 'IMAGE_QUALITY', 'Image Quality', ogr.OFTString, 0],
    ['dayOrNight', 'dayOrNight', 'Day/Night Indicator', ogr.OFTString, 0],
    ['sunEl', 'sunElevation', 'Sun Elevation L1', ogr.OFTReal, 0],
    ['sunAz', 'sunAzimuth', 'Sun Azimuth L1', ogr.OFTReal, 0],
    ['StartTime', 'sceneStartTime', 'Start Time', ogr.OFTDate, 0],
    ['StopTime', 'sceneStopTime', 'Stop Time', ogr.OFTDate, 0],
    ['UTM_ZONE', 'UTM_ZONE', 'UTM Zone', ogr.OFTInteger, 0],
    ['DATUM', 'DATUM', 'Datum', ogr.OFTString, 0],
    ['ELEVSOURCE', 'ELEVATION_SOURCE', 'Elevation Source', ogr.OFTString, 0],
    ['ELLIPSOID', 'ELLIPSOID', 'Ellipsoid', ogr.OFTString, 0],
    ['PROJ_L1', 'MAP_PROJECTION_L1', 'Map Projection Level-1', ogr.OFTString, 0],
    ['PROJ_L0RA', 'MAP_PROJECTION_L0RA', 'Map Projection L0Ra', ogr.OFTString, 0],
    ['ORIENT', 'ORIENTATION', 'Orientation', ogr.OFTString, 0],
    ['EPHEM_TYPE', 'EPHEMERIS_TYPE', 'Ephemeris Type', ogr.OFTString, 0],
    ['CPS_MODEL', 'GROUND_CONTROL_POINTS_MODEL', 'Ground Control Points Model', ogr.OFTInteger, 0],
    ['GCPSVERIFY', 'GROUND_CONTROL_POINTS_VERIFY', 'Ground Control Points Version', ogr.OFTInteger, 0],
    ['RMSE_MODEL', 'GEOMETRIC_RMSE_MODEL', 'Geometric RMSE Model (meters)', ogr.OFTReal, 0],
    ['RMSE_X', 'GEOMETRIC_RMSE_MODEL_X', 'Geometric RMSE Model X', ogr.OFTReal, 0],
    ['RMSE_Y', 'GEOMETRIC_RMSE_MODEL_Y', 'Geometric RMSE Model Y', ogr.OFTReal, 0],
    ['RMSEVERIFY', 'GEOMETRIC_RMSE_VERIFY', 'Geometric RMSE Verify', ogr.OFTReal, 0],
    ['FORMAT', 'OUTPUT_FORMAT', 'Output Format', ogr.OFTString, 0],
    ['RESAMP_OPT', 'RESAMPLING_OPTION', 'Resampling Option', ogr.OFTString, 0],
    ['LINES', 'REFLECTIVE_LINES', 'Reflective Lines', ogr.OFTInteger, 0],
    ['SAMPLES', 'REFLECTIVE_SAMPLES', 'Reflective Samples', ogr.OFTInteger, 0],
    ['TH_LINES', 'THERMAL_LINES', 'Thermal Lines', ogr.OFTInteger, 0],
    ['TH_SAMPLES', 'THERMAL_SAMPLES', 'Thermal Samples', ogr.OFTInteger, 0],
    ['PAN_LINES', 'PANCHROMATIC_LINES', 'Panchromatic Lines', ogr.OFTInteger, 0],
    ['PANSAMPLES', 'PANCHROMATIC_SAMPLES', 'Panchromatic Samples', ogr.OFTInteger, 0],
    ['GC_SIZE_R', 'GRID_CELL_SIZE_REFLECTIVE', 'Grid Cell Size Reflective', ogr.OFTInteger, 0],
    ['GC_SIZE_TH', 'GRID_CELL_SIZE_THERMAL', 'Grid Cell Size Thermal', ogr.OFTInteger, 0],
    ['GCSIZE_PAN', 'GRID_CELL_SIZE_PANCHROMATIC', 'Grid Cell Size Panchromatic', ogr.OFTInteger, 0],
    ['PROCSOFTVE', 'PROCESSING_SOFTWARE_VERSION', 'Processing Software Version', ogr.OFTString, 0],
    ['CPF_NAME', 'CPF_NAME', 'Calibration Parameter File', ogr.OFTString, 0],
    ['DATEL1_GEN', 'DATE_L1_GENERATED', 'Date L-1 Generated', ogr.OFTString, 0],
    ['GCP_Ver', 'GROUND_CONTROL_POINTS_VERSION', 'Ground Control Points Version', ogr.OFTInteger, 0],
    ['DatasetID', 'DatasetID', 'Dataset Identifier', ogr.OFTString, 0],
    ['CollectCat', 'COLLECTION_CATEGORY', 'Collection Category', ogr.OFTString, 0],
    ['CollectNum', 'COLLECTION_NUMBER', 'Collection Number', ogr.OFTString, 0],
    ['flightPath', 'flightPath', 'flightPath', ogr.OFTString, 0],
    ['RecStation', 'receivingStation', 'Station Identifier', ogr.OFTString, 0],
    ['imageQual1', 'imageQuality1', 'Image Quality 1', ogr.OFTString, 0],
    ['imageQual2', 'imageQuality2', 'Image Quality 2', ogr.OFTString, 0],
    ['gainBand1', 'gainBand1', 'Gain Band 1', ogr.OFTString, 0],
    ['gainBand2', 'gainBand2', 'Gain Band 2', ogr.OFTString, 0],
    ['gainBand3', 'gainBand3', 'Gain Band 3', ogr.OFTString, 0],
    ['gainBand4', 'gainBand4', 'Gain Band 4', ogr.OFTString, 0],
    ['gainBand5', 'gainBand5', 'Gain Band 5', ogr.OFTString, 0],
    ['gainBand6H', 'gainBand6H', 'Gain Band 6H', ogr.OFTString, 0],
    ['gainBand6L', 'gainBand6L', 'Gain Band 6L', ogr.OFTString, 0],
    ['gainBand7', 'gainBand7', 'Gain Band 7', ogr.OFTString, 0],
    ['gainBand8', 'gainBand8', 'Gain Band 8', ogr.OFTString, 0],
    ['GainChange', 'GainChange', 'Gain Change', ogr.OFTString, 0],
    ['GCBand1', 'gainChangeBand1', 'Gain Change Band 1', ogr.OFTString, 0],
    ['GCBand2', 'gainChangeBand2', 'Gain Change Band 2', ogr.OFTString, 0],
    ['GCBand3', 'gainChangeBand3', 'Gain Change Band 3', ogr.OFTString, 0],
    ['GCBand4', 'gainChangeBand4', 'Gain Change Band 4', ogr.OFTString, 0],
    ['GCBand5', 'gainChangeBand5', 'Gain Change Band 5', ogr.OFTString, 0],
    ['GCBand6H', 'gainChangeBand6H', 'Gain Change Band 6H', ogr.OFTString, 0],
    ['GCBand6L', 'gainChangeBand6L', 'Gain Change Band 6L', ogr.OFTString, 0],
    ['GCBand7', 'gainChangeBand7', 'Gain Change Band 7', ogr.OFTString, 0],
    ['GCBand8', 'gainChangeBand8', 'Gain Change Band 8', ogr.OFTString, 0],
    ['SCAN_GAP_I', 'SCAN_GAP_INTERPOLATION', 'Scan Gap Interpolation', ogr.OFTInteger, 0],
    ['ROLL_ANGLE', 'ROLL_ANGLE', 'Roll Angle', ogr.OFTReal, 0],
    ['FULL_PART', 'FULL_PARTIAL_SCENE', 'Full Partial Scene', ogr.OFTString, 0],
    ['NADIR_OFFN', 'NADIR_OFFNADIR', 'Nadir/Off Nadir', ogr.OFTString, 0],
    ['RLUT_FNAME', 'RLUT_FILE_NAME', 'RLUT File Name', ogr.OFTString, 0],
    ['BPF_N_OLI', 'BPF_NAME_OLI', 'Bias Parameter File Name OLI', ogr.OFTString, 0],
    ['BPF_N_TIRS', 'BPF_NAME_TIRS', 'Bias Parameter File Name TIRS', ogr.OFTString, 0],
    ['TIRS_SSM', 'TIRS_SSM_MODEL', 'TIRS SSM Model', ogr.OFTString, 0],
    ['TargetPath',  'Target_WRS_Path', 'Target WRS Path', ogr.OFTInteger, 0],
    ['TargetRow', 'Target_WRS_Row', 'Target WRS Row', ogr.OFTInteger, 0],
    ['DataAnom', 'data_anomaly', 'Data Anomaly', ogr.OFTString, 0],
    ['GapPSource', 'gap_phase_source', 'Gap Phase Source', ogr.OFTString, 0],
    ['GapPStat', 'gap_phase_statistic', 'Gap Phase Statistic', ogr.OFTReal, 0],
    ['L7SLConoff', 'scan_line_corrector', 'Scan Line Corrector', ogr.OFTString, 0],
    ['SensorAnom', 'sensor_anomalies', 'Sensor Anomalies', ogr.OFTString, 0],
    ['SensorMode', 'sensor_mode', 'Sensor Mode', ogr.OFTString, 0],
    ['browse', 'browseAvailable', 'Browse Available', ogr.OFTString, 0],
    ['browseURL', 'browseURL', 'browseUrl', ogr.OFTString, 0],
    ['MetadatUrl', 'metadataUrl', 'metadataUrl', ogr.OFTString, 0],
    ['FGDCMetdat', 'fgdcMetadataUrl', 'fgdcMetadataUrl', ogr.OFTString, 0],
    ['dataAccess', 'dataAccess', 'dataAccessUrl', ogr.OFTString, 0],
    ['orderUrl', 'orderUrl', 'orderUrl', ogr.OFTString, 0],
    ['DownldUrl', 'downloadUrl', 'downloadUrl', ogr.OFTString, 0]]

queryfieldnames = []
fnames = []

for element in fieldvaluelist:
    fnames.append(element[1])
    queryfieldnames.append(element[2])

if not os.access(ieo.catgpkg, os.F_OK):
    # Create geopackage
    data_source = driver.CreateDataSource(ieo.catgpkg)
else:
    data_source = driver.Open(ieo.catgpkg, 1)
layerpresent = False
layers = data_source.GetLayerCount()
if layers > 0:
    for i in range(layers):
        if layername == data_source.GetLayer(i).GetName():
            layerpresent = True
if not layerpresent:
    layer = data_source.CreateLayer(layername, target, ogr.wkbPolygon)
    for element in fieldvaluelist:
        field_name = ogr.FieldDefn(element[1], element[3])
        if element[4] > 0:
            field_name.SetWidth(element[4])
        layer.CreateField(field_name)

    layer.CreateField(ogr.FieldDefn('MaskType', ogr.OFTString)) # 'Fmask' or 'Pixel_QA'
    layer.CreateField(ogr.FieldDefn('Thumbnail_filename', ogr.OFTString))
    layer.CreateField(ogr.FieldDefn('Surface_reflectance_tiles', ogr.OFTString))
    layer.CreateField(ogr.FieldDefn('Brightness_temperature_tiles', ogr.OFTString))
    layer.CreateField(ogr.FieldDefn('Fmask_tiles', ogr.OFTString))
    layer.CreateField(ogr.FieldDefn('Pixel_QA_tiles', ogr.OFTString))
    layer.CreateField(ogr.FieldDefn('NDVI_tiles', ogr.OFTString))
    layer.CreateField(ogr.FieldDefn('EVI_tiles', ogr.OFTString))
    layer.CreateField(ogr.FieldDefn('Tile_filename_base', ogr.OFTString))
    
    args.migrate = True 

if args.migrate and os.path.isfile(shapefilepath):
    layer = migrate(layer, shapefilepath, fieldvaluelist, tiledir = args.tiledir, verbose = args.verbose)


#else:
lastupdate = None
lastmodifiedDate = None
shpfnames = []
updatemissing = []
badgeom = []
reimport = []
# Open existing shapefile with write access
data_source = driver.Open(ieo.catgpkg, 1)
layer = data_source.GetLayer(shapefile)
layerDefinition = layer.GetLayerDefn()
# Get list of field names
for i in range(layerDefinition.GetFieldCount()):
    shpfnames.append(layerDefinition.GetFieldDefn(i).GetName())
# Find missing fields and create them
for fname in fnames:
    if not fname in shpfnames:
        i = fnames.index(fname)
        field_name = ogr.FieldDefn(fnames[i], fieldvaluelist[i][3])
        if fieldvaluelist[i][4] > 0:
            field_name.SetWidth(fieldvaluelist[i][4])
        layer.CreateField(field_name)

# Iterate through features and fetch sceneID values
errors = {'total' : 0,
          'metadata' : 0,
          'date' : 0,
          'geometry' : 0}

featureCount = layer.GetFeatureCount()
if featureCount > 0:
    layer.StartTransaction()
    layer.GetNextFeature()
    while feature:
        
        datetuple = None
        try:
            sceneID = feature.GetField("sceneID")
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            if args.verbose:
                print(exc_type, fname, exc_tb.tb_lineno)
                print('ERROR: bad feature, deleting.')
            layer.DeleteFeature(feature.GetFID())
            ieo.logerror('{}/{}'.format(ieo.catgpkg, shapefile), '{} {} {}'.format(exc_type, fname, exc_tb.tb_lineno), errorfile = errorfile)
            feature = layer.GetNextFeature()
            continue
        scenelist.append(sceneID)
        if not feature.GetField('SensorID') in ['TM', 'ETM', 'OLI', 'TIRS', 'OLI_TIRS']:
            if args.verbose:
                print('ERROR: missing metadata for SceneID {}. Feature will be deleted from shapefile and reimported.'.format(sceneID))
            ieo.logerror(sceneID, 'Feature missing metadata, deleted, reimportation required.')
            reimport.append(datetime.datetime.strptime(sceneID[9:16], '%Y%j'))
            layer.DeleteFeature(feature.GetFID())
            errors['total'] += 1
            errors['metadata'] += 1
            
        else:    
            try:
                mdate = feature.GetField('dateUpdated')
                datetuple = datetime.datetime.strptime(mdate, '%Y/%m/%d')
                if not lastupdate or datetuple > lastupdate:
                    lastupdate = datetuple
                    lastmodifiedDate = mdate
            except Exception as e:
                if args.verbose:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno)
                    print('ERROR: modifiedDate information missing for SceneID {}, adding to list.'.format(sceneID))
                ieo.logerror(sceneID, 'Modification date missing.', errorfile = errorfile)
                updatemissing.append(sceneID)
                errors['total'] += 1
                errors['date'] += 1
            
            
            try:
                geom = feature.GetGeometryRef()
                env = geom.GetEnvelope()
                if env[0] == env[1] or env[2] == env[3]:
                    if args.verbose:
                        print('Bad geometry identified for SceneID {}, adding to the list.'.format(sceneID))
                    ieo.logerror(sceneID, 'Bad/ missing geometry.')
                    badgeom.append(sceneID)
            except Exception as e:
                if args.verbose:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno)
                    print('Bad geometry identified for SceneID {}, adding to the list.'.format(sceneID), errorfile = errorfile)
                ieo.logerror(sceneID, 'Bad/ missing geometry.')
                badgeom.append(sceneID)
                errors['total'] += 1
                errors['geometry'] += 1
        if errors['total'] > 0 and (errors['total'] % 100 == 0):
            print('{} errors found in layer of types: metadata: {}, missing modification date: {}, missing/ bad geometry: {}.'.format(errors['total'], errors['metadata'], errors['date'], errors['geometry']))
        feature = layer.GetNextFeature()
    layer.CommitTransaction()

if len(reimport) > 0 and lastupdate:
    if min(reimport) < lastupdate:
        lastmodifiedDate = datetime.datetime.strftime('%Y-%m-%d', min(reimport))

fielddict = {'Brightness_temperature_tiles' : {'ext' : '_BT_{}.dat'.format(ieo.projacronym), 'dirname' : ieo.btdir},
            'CFmask_tiles' : {'ext' : '_cfmask.dat', 'dirname' : ieo.fmaskdir},
            'Pixel_QA_tiles' : {'ext' : '_pixel_qa.dat', 'dirname' : ieo.pixelqadir},
            'NDVI_tiles' : {'ext' : '_NDVI.dat', 'dirname' : ieo.ndvidir},
            'EVI_tiles' : {'ext' : '_EVI.dat', 'dirname' : ieo.evidir}}

thumbnails = []
scenes = []
filenum = 1

# get apiKey for USGS EarthExplorer query
apiKey = getapiKey()

# run query

scenedict = scenesearch(apiKey, scenelist, updatemissing, badgeom, lastmodifiedDate)
sceneIDs = scenedict.keys()
print('Total scenes to be added or updated to shapefile: {}'.format(len(sceneIDs)))

if len(sceneIDs) > 0:
    for sceneID in sceneIDs:
        print('Processing {}, scene number {} of {}.'.format(sceneID, filenum, len(sceneIDs)))
        if not (scenedict[sceneID]['updategeom'] or scenedict[sceneID]['updatemodifiedDate']) and ('coords' in scenedict[sceneID].keys()):
            scenedict = findlocalfiles(sceneID, fielddict, scenedict)
            if scenedict[sceneID]['browseUrl'].endswith('.jpg'):
                dlurl = scenedict[sceneID]['browseUrl']
                thumbnails.append(scenedict[sceneID]['browseUrl'])
    
            print('\nAdding {} to shapefile.'.format(sceneID))
            scenelist.append(sceneID)
            # create the feature
            feature = ogr.Feature(layer.GetLayerDefn())
            # Add field attributes
            feature.SetField('sceneID', sceneID)
            for key in scenedict[sceneID].keys():
                if (scenedict[sceneID][key]) and key in queryfieldnames:
                    try:
                        if fieldvaluelist[queryfieldnames.index(key)][3] == ogr.OFTDate:
                            if isinstance(scenedict[sceneID][key], str):
                                if '/' in scenedict[sceneID][key]:
                                    scenedict[sceneID][key] = scenedict[sceneID][key].replace('/', '-')
                                scenedict[sceneID][key] = datetime.datetime.strptime(scenedict[sceneID][key], '%Y-%m-%d')
                            feature.SetField(fnames[queryfieldnames.index(key)], scenedict[sceneID][key].year, scenedict[sceneID][key].month, scenedict[sceneID][key].day, scenedict[sceneID][key].hour, scenedict[sceneID][key].minute, scenedict[sceneID][key].second, 100)
                        else:
                            feature.SetField(fnames[queryfieldnames.index(key)], scenedict[sceneID][key])
                    except Exception as e:
                        if args.verbose:
                            exc_type, exc_obj, exc_tb = sys.exc_info()
                            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                            print(exc_type, fname, exc_tb.tb_lineno)
                            print('Error with SceneID {}, fieldname = {}, value = {}: {}'.format(sceneID, fnames[queryfieldnames.index(key)], scenedict[sceneID][key], e))
                        ieo.logerror(key, e, errorfile = errorfile)
            
            coords = scenedict[sceneID]['coords']
            # Create ring
            ring = ogr.Geometry(ogr.wkbLinearRing)
            for coord in coords:
                ring.AddPoint(coord[0], coord[1])
            if not coord[0] == coords[0][0] and coord[1] == coords[0][1]:
                ring.AddPoint(coord[0][0], coord[0][1])
            # Create polygon
            
            poly = ogr.Geometry(ogr.wkbPolygon)
    
            poly.AddGeometry(ring)
            poly.Transform(transform)   # Convert to local projection
            feature.SetGeometry(poly)
            basename = os.path.basename(dlurl)
            jpg = os.path.join(jpgdir, basename)
            if not os.access(jpg, os.F_OK) and args.thumbnails:
                try:
                    response = dlthumb(dlurl, jpgdir)
                    if response == 'Success!':
                        geom = feature.GetGeometryRef()
                        print('Creating world file.')
                        makeworldfile(jpg, poly)
                        print('Migrating world and projection files to new directory.')
                        jpw = jpg.replace('.jpg', '.jpw')
                        prj = jpg.replace('.jpg', '.prj')
                    else:
                        print('Error with sceneID or filename, adding to error list.')
                        ieo.logerror(sceneID, response, errorfile = errorfile)
                        errorsfound = True
                    if os.access(jpg, os.F_OK):
                        feature.SetField('Thumbnail_filename', jpg)

                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno)
                    ieo.logerror(os.path.basename(jpg), e, errorfile = errorfile)
                    errorsfound = True
            layer.CreateFeature(feature)
            feature.Destroy()
        else:
            layer.ResetReading()
            for feature in layer:
                if feature.GetField('sceneID') == sceneID:
                    if scenedict[sceneID]['updategeom']: 
                        print('Updating geometry for SceneID {}.'.format(sceneID))
                        coords = scenedict[sceneID]['coords']
                        # Create ring
                        ring = ogr.Geometry(ogr.wkbLinearRing)
                        for coord in coords:
                            ring.AddPoint(coord[0], coord[1])
                        if not coord[0] == coords[0][0] and coord[1] == coords[0][1]:
                            ring.AddPoint(coord[0][0], coord[0][1])
                        # Create polygon
                        
                        poly = ogr.Geometry(ogr.wkbPolygon)
                
                        poly.AddGeometry(ring)
                        poly.Transform(transform)   # Convert to local projection
                        feature.SetGeometry(poly)
                        
                    if scenedict[sceneID]['updatemodifiedDate']:
#                        try:
                        print('Updating modification date for SceneID {}.'.format(sceneID))
#                        if  isinstance(scenedict[sceneID]['modifiedDate'], str):
#                            scenedict[sceneID]['modifiedDate'] = datetime.datetime.strptime(scenedict[sceneID]['modifiedDate'], '%Y-%m-%d')
                        feature.SetField('dateUpdated', scenedict[sceneID]['modifiedDate'])
#                        except Exception as e:
#                            exc_type, exc_obj, exc_tb = sys.exc_info()
#                            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
#                            print(exc_type, fname, exc_tb.tb_lineno)
#                            print('ERROR: modifiedDate information ("{}") not set for SceneID {}, adding to list.'.format(scenedict[sceneID]['modifiedDate'], sceneID))
#                            ieo.logerror(sceneID, 'Error setting "Updated" field.')
                    layer.SetFeature(feature)
                    feature.Destroy()
#        print('\n')
        filenum += 1
    
data_source = None

print('Processing complete.')

