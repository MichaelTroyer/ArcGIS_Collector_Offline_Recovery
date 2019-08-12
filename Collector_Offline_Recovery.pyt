# -*- coding: utf-8 -*-

"""
Author:
Michael Troyer

Date:
7/29/2019


###### Purpose: ######

Convert a runtime (Collector) geodatabase to a file geodatabase and sync with an
existing hosted feature service.

MUST RUN IN ArcPRO!

###### Parameters: ######

Inputs:
    * Path to input runtime geodatabase folder  [workspace]
    * Organization URL                          [string]
    * Username                                  [string]
    * Password                                  [string hidden]
    * Feature Service Name                      [string]

Outputs:
    * None


###### Process: ######

01. Get paths to Collector offline data folder (copied from device) and output files
02. Create xml workspace file in Collector offline data folder
03. Create temp fgdb in Collector offline data folder
04. Import xml to temp fgdb, capturing schema and data

05. Connect to the GIS and get the hosted feature layers

For each hosted feature layer:
    06. Make feature layer (from corresponding feature class in temp_gdb)
    07. Selet local layer by hosted layer intersection (get features not in service)
    08. Append selected records to hosted feature layer



#TODO: Sweep attributes for updates
#TODO: Sweep for new photos
#TODO: What about intersection of edited polygons?

#TODO: Add SAML Auth handler

"""

import logging
import os
import sys
import traceback

import arcpy
import arcgis


arcpy.env.addOutputsToMap = False
arcpy.env.overwriteOutput = True

# Configure the logger
fname = os.path.splitext(__file__)[0]
log_file ='{}.log'.format(fname)
logging.basicConfig(
    filename=log_file,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    )


###### Main Program: ######

class Toolbox(object):
    def __init__(self):
        self.label = "Collector Offline Data Sync"
        self.alias = "Collector_Offline_Data_Sync"
        self.tools = [CollectorOfflineDataSync]


class CollectorOfflineDataSync(object):
    def __init__(self):
        self.label = "Sync_Collector_Data"
        self.description = "Sync Collector Data"
        self.canRunInBackground = True 

    def getParameterInfo(self):

        offline_data_folder = arcpy.Parameter(
            displayName="Offline Data Folder Location",
            name="offline_data_folder",
            datatype="DEFolder",
            parameterType="Required",
            direction="Input",
            )

        organization_url = arcpy.Parameter(
            displayName="Organization URL",
            name="organization_url",
            datatype="String",
            parameterType="Required",
            direction="Input",
            )

        username = arcpy.Parameter(
            displayName="Username",
            name="username",
            datatype="String",
            parameterType="Required",
            direction="Input",
            )

        password = arcpy.Parameter(
            displayName="Password",
            name="password",
            datatype="String Hidden",
            parameterType="Required",
            direction="Input",
            )

        feature_server_url = arcpy.Parameter(
            displayName="Feature Server URL",
            name="feature_server_url",
            datatype="String",
            parameterType="Required",
            direction="Input",
            )

        debug_mode = arcpy.Parameter(
            displayName="Debug Mode (sets logging level to DEBUG and outputs data without appending)",
            name="debug_mode",
            datatype="Boolean",
            parameterType="Optional",
            direction="Input",
            )

        return [
            offline_data_folder,
            organization_url,
            username,
            password,
            feature_server_url,
            debug_mode
            ]

    def isLicensed(self):
        return True

    def updateParameters(self, params):
        return

    def updateMessages(self, params):
        offline_data_folder = params[0]
        offline_files = os.listdir(offline_data_folder.valueAsText)
        geodatabase_files = [f for f in offline_files if os.path.splitext(f)[1] == '.geodatabase']
        if not geodatabase_files:
            offline_data_folder.setErrorMessage(
                'No .geodatabase file found in folder:\n{}'.format(offline_data_folder.valueAsText)
            )
        elif len(geodatabase_files) > 1:
            offline_data_folder.setErrorMessage(
                'Multiple .geodatabase files found in folder:\n{}\n'.format(offline_data_folder.valueAsText)
            )
        return

    def execute(self, params, messages):

        try:
            offline_data_folder, organization_url, username, password, feature_server_url, debug_mode = params

            logging.info('Offline data folder: {}'.format(offline_data_folder.valueAsText))
            logging.info('Organization URL: {}'.format(organization_url.valueAsText))
            logging.info('Username: {}'.format(username.valueAsText))
            logging.info('Feature service URL: {}'.format(feature_server_url.valueAsText))
            logging.info('Debug Mode: {}'.format(debug_mode.valueAsText))

            if debug_mode.value:
                logging.getLogger().setLevel(logging.DEBUG)


########### Step 01. Get paths to Collector offline data folder (copied from device) and output files
            offline_data_folder = offline_data_folder.valueAsText
            offline_files = os.listdir(offline_data_folder)

            geodatabase_file = os.path.join(
                offline_data_folder,
                [f for f in offline_files if os.path.splitext(f)[1] == '.geodatabase'][0]
            )
            logging.info('Geodatabase file: {}'.format(geodatabase_file))

            temp_gdb = os.path.join(offline_data_folder, 'sync_temp.gdb')
            temp_xml = os.path.join(offline_data_folder, 'sync_temp.xml')

########### Step 02. Create xml workspace file in Collector offline data folder
            arcpy.ExportXMLWorkspaceDocument_management(
                in_data=geodatabase_file,
                out_file=temp_xml,
                export_type='DATA'
                )

########### Step 03. Create temp fgdb in Collector offline data folder
            arcpy.CreateFileGDB_management(offline_data_folder, 'sync_temp.gdb')

########### Step 04. Import xml to temp fgdb, capturing schema and data
            arcpy.ImportXMLWorkspaceDocument_management(temp_gdb, temp_xml)

########### Step 05. Connect to the GIS and get the hosted feature layers
            gis = arcgis.gis.GIS(
                url=organization_url.valueAsText,
                username=username.valueAsText,
                password=password.valueAsText,
                )
            feature_layers = arcgis.features.FeatureLayerCollection(
                url=feature_server_url.valueAsText,
                gis=gis,
                )

########### For each hosted feature layer/ feature class combo:
            for feature_layer in feature_layers.layers:
                feature_layer_name = feature_layer.properties.name
                arcpy.AddMessage('Syncing: {}'.format(feature_layer_name))
                logging.info('Syncing: {}'.format(feature_layer_name))

                # This is kind of convoluted, but it works. As far as I can tell, HFS have to be cast as an
                # arcpy FeatureSet or arcpy FeatureLayer of a FeatureSet in order to be consumed by arcpy geoprocessing tools
                feature_set = arcpy.FeatureSet(feature_layer.url)
                feature_set_layer = arcpy.MakeFeatureLayer_management(feature_set, 'in_memory\\fl_tmp')

                ### Step 06. Make feature layer (from corresponding feature class in temp_gdb)
                fc_path = os.path.join(temp_gdb, feature_layer_name)
                if not arcpy.Exists(fc_path):
                    errMsg = 'Source features not found:\n{}\nDid you reference the correct feature service?'.format(fc_path)
                    arcpy.AddError(errMsg)
                    logging.info(errMsg)
                fc_layer = arcpy.MakeFeatureLayer_management(fc_path, 'in_memory\\fc_tmp')

                ### Step 07. Select local layer by hosted layer intersection (get features not in service)
                arcpy.SelectLayerByLocation_management(
                    in_layer=fc_layer,
                    overlap_type='INTERSECT',
                    select_features=feature_set_layer,
                    search_distance='2 METERS',  # To account for differences in projections, this is risky business
                    selection_type="NEW_SELECTION",
                    invert_spatial_relationship="INVERT"
                    )

                ### Step 08. Append selected records to hosted feature layer
                # If no selection, skip
                selections = arcpy.Describe(fc_layer).FIDSet
                if selections:
                    new_data = '{}_updates'.format(feature_layer_name)
                    if debug_mode.value:
                        arcpy.CopyFeatures_management(fc_layer, os.path.join(temp_gdb, new_data))
                    else:
                        arcpy.Append_management(fc_layer, feature_set_layer, 'NO_TEST')  # Sometimes TEST is too picky (order)
                else:
                    arcpy.AddMessage('{} is current..'.format(feature_layer_name))
                    logging.info('{} is current..'.format(feature_layer_name))

        except:
            arcpy.AddError(traceback.format_exc())
            logging.exception("An error occurred..")

        finally:
            try:
                logging.shutdown()
                # arcpy.Delete_management(temp_gdb)
                arcpy.Delete_management(temp_xml)
                pass
            except:
                pass  # These may not delete

        return
