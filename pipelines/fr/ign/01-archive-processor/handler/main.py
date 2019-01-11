import os
import json
import shutil
import threading
import re
import requests
import time
import traceback

from pyunpack import Archive
import fiona

from confluent_kafka import Producer, KafkaError
from urllib.request import urlopen

def handler(context, event):

    b = event.body
    if not isinstance(b, dict):
        body = json.loads(b.decode('utf-8-sig'))
    else:
        body = b
    
    context.logger.info("Event received: " + str(body))

    try:

        # if we're not ready to handle this request yet, deny it
        if not FunctionState.done_loading:
            context.logger.warn_with('Function not ready, denying request !')
            raise NuclioResponseError(
                'The service is loading and is temporarily unavailable.',requests.codes.unavailable)
        
        # parse event's payload
        b = Helpers.parse_body(context,body)

        archive_format = b['archive_format']
        archive_url = b['archive_url']
        source_id = b['source_id']
        hydro_id_field = b['hydro_id_field']
        normalized_hydro_properties = b['normalized_hydro_properties']
        version = b['version']

        # create temporary directory
        tmp_dir = Helpers.create_temporary_dir(context,event)

        archive_name = archive_url.rsplit('/',1)[-1]
        # Workaround for the new archives published after 2016
        special_format = '.001'
        if archive_name.endswith("{0}{1}".format(archive_format,special_format)):
            archive_name = archive_name[:-len(special_format)]

        archive_target_path = os.path.join(tmp_dir,archive_name)

        # download requested archive
        context.logger.info("Start downloading archive '{0}' ...".format(archive_url))
        Helpers.download_file(context,archive_url,archive_target_path)

        # extract archive
        context.logger.info("Start extracting archive ...")
        Archive(archive_target_path).extractall(tmp_dir)
        context.logger.info("Archive successfully extracted !")

        # search shapefiles
        shpfs = []
        directory, _ = os.path.splitext(archive_name)
        search_path = os.path.join(tmp_dir,directory)
        target_files = FunctionConfig.target_files
        for dirpath, dirnames, files in os.walk(search_path):
            for f in [file for file in files if file in target_files.keys()]:
                shpfs.append(os.path.join(dirpath,f))

        # check if we have the expected number of shapefiles
        if not len(shpfs) == len(target_files):
            context.logger.warn_with("Expected to find '{0}' files in archive '{1}', found={2}"
                .format(target_files,archive_name,shpfs))
            raise NuclioResponseError(
                "Expected to find '{0}' files in archive '{1}', found={2}".format(target_files,archive_name,shpfs),
                requests.codes.bad)

        # shapefile CRS (hard coded EPSG code ...)
        region = source_id.rsplit(':',1)[-1]
        crs_code = FunctionConfig.region_code_epsg_crs[region]
        context.logger.info("Coordinate Reference System, EPSG:{0} ...".format(crs_code))

        # kafka settings
        p = FunctionState.producer
        target_topic = FunctionConfig.target_topic

        # function to strip z value (coordinate) if exists
        def strip_z(coords):
            if isinstance(coords,list):
                return [strip_z(c) for c in coords]
            else:
                return (coords[0],coords[1])

        for f in shpfs:
            # read shapfile 
            reader = fiona.open(f)
            data_type = FunctionConfig.target_files[os.path.basename(f)]

            # process shapfile records
            count = 0
            context.logger.info('Processing shapefile {0} ...'.format(os.path.basename(f)))
            start = time.time()
            for sr in reader:
                geom_properties = dict(sr['properties'])
                geom_id = "{0}:{1}".format(source_id,geom_properties[hydro_id_field])

                properties = {
                    "hydro": geom_properties,
                    "crs": {
                        "type": "EPSG",
                        "properties": {
                            "code": crs_code
                        }
                    },
                    "version": version
                }

                if 'NOM' in geom_properties and isinstance(geom_properties['NOM'],bytes):
                    geom_properties['NOM'] = geom_properties['NOM'].decode('ISO-8859-1')

                for prop in normalized_hydro_properties:
                    value = geom_properties.get(normalized_hydro_properties[prop]['sourceProp'])
                    if 'defaultOrigValueToDelete' in normalized_hydro_properties[prop]:
                        default_value = normalized_hydro_properties[prop]['defaultOrigValueToDelete']
                        if value == default_value:
                            continue
                    # Get value in SI base unit if needed
                    if 'coefSI' in normalized_hydro_properties[prop]:
                        value = eval("{0}{1}".format(value,normalized_hydro_properties[prop]['coefSI']))
                    if 'valueMap' in normalized_hydro_properties[prop]:
                        if value in normalized_hydro_properties[prop]['valueMap']:
                            value =  normalized_hydro_properties[prop]['valueMap'][value]
                    if 'evalMethod' in normalized_hydro_properties[prop]:
                        value = getattr(value,normalized_hydro_properties[prop]['evalMethod'])()
                    if value:
                        properties[prop] = value

                properties['type'] = data_type

                geometry = sr['geometry']
                # strip the z value of each coordinate
                geometry['coordinates'] = strip_z(geometry['coordinates'])

                # forge a GeoJSON feature from the shape record
                hydro = dict(_id=geom_id, type="Feature", geometry=geometry, properties=properties)

                # send the GeoJSON feature
                value = json.dumps(hydro).encode("utf-8-sig")
                p.produce(target_topic, value)
                if count % 1000 == 0:
                    p.flush()

                count += 1

            p.flush()
            context.logger.info('Shapefile {0} processed successfully ({1} geometries in {2}s).'
                .format(os.path.basename(f),count,round(time.time()-start)))

    except NuclioResponseError as error:
        return error.as_response(context)

    except Exception as error:
        context.logger.warn_with('Unexpected error occurred, responding with internal server error',
            exc=str(error))
        message = 'Unexpected error occurred: {0}\n{1}'.format(error, traceback.format_exc())
        return NuclioResponseError(message).as_response(context)

    finally:
        shutil.rmtree(tmp_dir)

    return "Processing Completed ({0} geometries)".format(count)


class FunctionConfig(object):

    kafka_bootstrap_server = None

    target_topic = None

    accepted_source_id = 'hydro:fr'

    region_code_epsg_crs = dict([
        ('GLP',32620),
        ('MTQ',32620),
        ('GUF',2972),
        ('REU',2975),
        ('SPM',4467),
        ('MYT',4471),
        ('SBA',32620),
        ('SMA',32620),
        ('FXX',2154)])

    target_files = dict([
        ("SURFACE_EAU.SHP", "WATER_AREA"),
        ("TRONCON_COURS_EAU.SHP","WATER_COURSE")])

class FunctionState(object):

    producer = None

    done_loading = False


class Helpers(object):

    @staticmethod
    def load_configs():

        FunctionConfig.kafka_bootstrap_server = os.getenv('KAFKA_BOOTSTRAP_SERVER')

        FunctionConfig.target_topic = os.getenv('TARGET_TOPIC')

    @staticmethod
    def load_producer():

        p = Producer({
            'bootstrap.servers':  FunctionConfig.kafka_bootstrap_server})

        FunctionState.producer = p

    @staticmethod
    def parse_body(context, body):

        # parse archive's format
        if not ('format' in body and body['format'].lower() == '7z'):
            context.logger.warn_with('Archive\'s \'format\' must be \'7z\' !')
            raise NuclioResponseError(
                'Archive\'s \'format\' must be \'7z\' !',requests.codes.bad)
        archive_format = body['format']

        # parse archive's URL
        if not 'url' in body:
            context.logger.warn_with('Missing \'url\' attribute !')
            raise NuclioResponseError(
                'Missing \'url\' attribute !',requests.codes.bad)
        archive_url = body['url']

        # parse archive's sourceID
        if not 'sourceID' in body:
            context.logger.warn_with('Missing \'sourceID\' attribute !')
            raise NuclioResponseError(
                'Missing \'sourceID\' attribute !',requests.codes.bad)

        # WORKAROUND: Check sourceID to only accept RPG archives (IGN/France)
        # TODO: generalize ....
        if not body['sourceID'].startswith(FunctionConfig.accepted_source_id):
            context.logger.warn_with("sourceID: not a '{0}' data source !".format(FunctionConfig.accepted_source_id))
            raise NuclioResponseError(
                "sourceID: not a '{0}' data source !".format(FunctionConfig.accepted_source_id),requests.codes.bad)
        source_id = body['sourceID']

        # parse archive's parcelIdField
        if not 'hydroIdField' in body:
            context.logger.warn_with('Missing \'hydroIdField\' attribute !')
            raise NuclioResponseError(
                'Missing \'hydroIdField\' attribute !',requests.codes.bad)
        hydro_id_field = body['hydroIdField']

        # parse archive's normalizedProperties
        if not 'normalizedProperties' in body:
            context.logger.warn_with('Missing \'normalizedProperties\' attribute !')
            raise NuclioResponseError(
                'Missing \'normalizedProperties\' attribute !',requests.codes.bad)

        if not isinstance(body['normalizedProperties'],dict):
            context.logger.warn_with('\'normalizedProperties\' is not a dict !')
            raise NuclioResponseError(
                '\'normalizedProperties\' is not a dict !',requests.codes.bad)
        normalized_hydro_properties = body['normalizedProperties']

        # parse archive's version
        if not 'version' in body:
            context.logger.warn_with('Missing \'version\' attribute !')
            raise NuclioResponseError(
                'Missing \'version\' attribute !',requests.codes.bad)
        version = str(body['version'])

        b = {
            "archive_format": archive_format,
            "archive_url": archive_url,
            "source_id": source_id,
            "hydro_id_field": hydro_id_field,
            "normalized_hydro_properties": normalized_hydro_properties,
            "version": version
        }

        return b

    @staticmethod
    def create_temporary_dir(context, event):
        """
        Creates a uniquely-named temporary directory (based on the given event's id) and returns its path.
        """
        temp_dir = '/tmp/nuclio-event-{0}'.format(event.id)
        os.makedirs(temp_dir)

        context.logger.debug_with('Temporary directory created', path=temp_dir)

        return temp_dir

    @staticmethod
    def download_file(context, url, target_path):
        """
        Downloads the given remote URL to the specified path.
        """
        # make sure the target directory exists
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        try:
            chunk_size = 8192
            with urlopen(url) as response:
                with open(target_path, 'wb') as f:
                    while True:
                        chunk = response.read(chunk_size)
                        if not chunk:
                            break
                        f.write(chunk)
        except Exception as error:
            if context is not None:
                context.logger.warn_with('Failed to download file',
                                         url=url,
                                         target_path=target_path,
                                         exc=str(error))
            raise NuclioResponseError('Failed to download file: {0}'.format(url),
                                      requests.codes.service_unavailable)
        if context is not None:
            context.logger.info_with('File downloaded successfully',
                                     size_bytes=os.stat(target_path).st_size,
                                     target_path=target_path)

    @staticmethod
    def on_import():

        Helpers.load_configs()
        Helpers.load_producer()
        FunctionState.done_loading = True


class NuclioResponseError(Exception):

    def __init__(self, description, status_code=requests.codes.internal_server_error):
        self._description = description
        self._status_code = status_code

    def as_response(self, context):
        return context.Response(body=self._description,
                                headers={},
                                content_type='text/plain',
                                status_code=self._status_code)


t = threading.Thread(target=Helpers.on_import)
t.start()
