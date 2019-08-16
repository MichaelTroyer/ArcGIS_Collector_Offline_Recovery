# -*- coding: utf-8 -*-

"""
Author:
Michael Troyer

Date:
7/29/2019


###### Purpose: ######

Convert a runtime (Collector) geodatabase to a file geodatabase and sync with an
existing hosted feature service.

Will only add new data and updated existing data - will not delete service data - too dangerous..


###### Requirements: ######

MUST RUN IN ArcPRO!
Feature Service layers must have 'GlobalID', and 'last_edited_date' in order to sync..


###### Parameters: ######

Inputs:
    * Path to input runtime geodatabase file    [file]
    * Organization URL                          [string]
    * Username                                  [string]
    * Password                                  [string hidden]
    * Feature Service Name                      [string]

Outputs:
    * None


###### Process: ######
Copy .geodatabase to XML
Create file geodatabase and import XML data
Connect to feature service and get layers
For each layer, get the corresponding feature class from geodatabase
Compare the global IDs and last_edit_dates to get adds (new globals) and updates (matching globals with more recent edits)
Remove rows to be updated from service
Add new features and updated features
Profit.
...


###### TODO: ######
* Figure out how to handle updates to attachments only (add/delete) - does not update last_edit_date

"""

import logging
import os
import sys
import traceback

import arcpy
import arcgis


arcpy.env.addOutputsToMap = False
arcpy.env.overwriteOutput = True
# Only works for Enterprise data.. Not sure if works for feature services at all..
arcpy.env.preserveGlobalIds = True


###### Helpers: ######

def deleteInMemory():
    """
    Delete in memory tables and feature classes.
    Reset to original worksapce when done.
    """
    # get the original workspace location
    orig_workspace = arcpy.env.workspace
    # Set the workspace to in_memory
    arcpy.env.workspace = "in_memory"
    # Delete all in memory feature classes
    for fc in arcpy.ListFeatureClasses():
        arcpy.Delete_management(fc)
    # Delete all in memory tables
    for tbl in arcpy.ListTables():
        arcpy.Delete_management(tbl)
    # Reset the workspace
    arcpy.env.workspace = orig_workspace


def get_global_w_last_edit_date(fc):
    """
    returns a dictionary of globalID and last edit date for each 
    feature in a feature class.

    Assumes standard 'GlobalID', and 'last_edited_date' field names!
    """ 
    query_fields = ['GlobalID', 'last_edited_date']
    try:
        return {row[0]: row[1] for row in arcpy.da.SearchCursor(fc, query_fields)}
    except:
        raise Exception('Input feature class does not have GlobalID and last_edited_date fields - cannot sync..')


def buildWhereClauseFromList(table, field, valueList):
    """
    Takes a list of values and constructs a SQL WHERE
    clause to select those values within a given field and table.
    """
    # Add DBMS-specific field delimiters
    fieldDelimited = arcpy.AddFieldDelimiters(arcpy.Describe(table).path, field)
    # Determine field type
    fieldType = arcpy.ListFields(table, field)[0].type
    # Add single-quotes for string field values
    if str(fieldType) in ('String', 'GlobalID'):
        valueList = ["'%s'" % value for value in valueList]
    # Format WHERE clause in the form of an IN statement
    whereClause = "%s IN(%s)" % (fieldDelimited, ', '.join(map(str, valueList)))
    return whereClause


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

        geodatabase_file = arcpy.Parameter(
            displayName="Input .geodatabase file",
            name="geodatabase_file",
            datatype="DEFile",
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
            displayName="Debug Mode (copies insert and update data without modifying service)",
            name="debug_mode",
            datatype="Boolean",
            parameterType="Optional",
            direction="Input",
            )

        return [
            geodatabase_file,
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
        geodatabase_file = params[0]
        if geodatabase_file.value:
            if not os.path.splitext(geodatabase_file.valueAsText)[1].lower() == '.geodatabase':
                geodatabase_file.setErrorMessage('{} is not a .geodatabase file..'.format(geodatabase_file.valueAsText))
        return

    def execute(self, params, messages):

        try:
            deleteInMemory()

            geodatabase_file, organization_url, username, password, feature_server_url, debug_mode = params
            geodatabase_file = geodatabase_file.valueAsText

            # Get the geodatabase file parent directory
            parent_dir = os.path.dirname(geodatabase_file)
            geodatabase_filename = os.path.basename(geodatabase_file)
            geodatabase_name = os.path.splitext(geodatabase_filename)[0]

            # Configure the logger
            log_file = os.path.join(parent_dir, '{}.log'.format(geodatabase_name))
            logging.basicConfig(
                filename=log_file,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                level=logging.INFO,
                )

            def log_message(msg):
                arcpy.AddMessage(msg); logging.info(msg)

            logging.info('Godatabase_file: {}'.format(geodatabase_file))
            logging.info('Organization URL: {}'.format(organization_url.valueAsText))
            logging.info('Username: {}'.format(username.valueAsText))
            logging.info('Feature service URL: {}'.format(feature_server_url.valueAsText))
            logging.info('Debug Mode: {}'.format(debug_mode.valueAsText))

            if debug_mode.value:
                logging.getLogger().setLevel(logging.DEBUG)
                msg = 'Debug Mode - Copying data only..'
            else:
                msg = 'Update Mode - Updating service layers..'
            log_message(msg)

            # Get output file paths
            temp_gdb = os.path.join(parent_dir, '{}.gdb'.format(geodatabase_name))
            temp_xml = os.path.join(parent_dir, '{}.xml'.format(geodatabase_name))

            # Create xml workspace file in geodatabase file directory
            arcpy.ExportXMLWorkspaceDocument_management(
                in_data=geodatabase_file,
                out_file=temp_xml,
                export_type='DATA'
                )

            # Create temp fgdb in geodatabase file directory
            arcpy.CreateFileGDB_management(parent_dir, os.path.basename(temp_gdb))

            # Import xml to temp fgdb, capturing schema and data
            arcpy.ImportXMLWorkspaceDocument_management(temp_gdb, temp_xml)

            # Connect to the GIS and get the hosted feature layers
            gis = arcgis.gis.GIS(
                url=organization_url.valueAsText,
                username=username.valueAsText,
                password=password.valueAsText,
                )
            feature_layers = arcgis.features.FeatureLayerCollection(
                url=feature_server_url.valueAsText,
                gis=gis,
                )

            # For each hosted feature layer/ feature class combo:
            for feature_layer in feature_layers.layers:
                try:
                    feature_layer_name = feature_layer.properties.name
                    msg = 'Processing feature class: {}'.format(feature_layer_name)
                    log_message(msg)

                    # This is kind of convoluted, but it works. As far as I can tell, HFS have to be cast as an
                    # arcpy FeatureSet or arcpy FeatureLayer of a FeatureSet in order to be consumed by arcpy geoprocessing tools
                    feature_set = arcpy.FeatureSet(feature_layer.url)
                    feature_service_layer = arcpy.MakeFeatureLayer_management(feature_set, 'in_memory\\feature_service_tmp')

                    # Make feature layer (from corresponding feature class in temp_gdb)
                    feature_class_path = os.path.join(temp_gdb, feature_layer_name)
                    if not arcpy.Exists(feature_class_path):
                        errMsg = 'Source features not found:\n{}\nDid you reference the correct feature service?'.format(feature_class_path)
                        log_message(errMsg)
                    feature_class_layer = arcpy.MakeFeatureLayer_management(feature_class_path, 'in_memory\\feature_class_tmp')

                    # Get the GlobalIDs and last edit dates
                    feature_service_globals = get_global_w_last_edit_date(feature_service_layer)
                    feature_class_globals = get_global_w_last_edit_date(feature_class_layer)

                    # Converting to and from XML uppers the GlobalIDs! Why XML, WHY??
                    # That requires that we add a case translation mechanism before making compaisons and attribute selections..
                    fsg_translator = {fsg.lower(): fsg  for fsg in feature_service_globals}

                    # INSERTS: These globalIDs are new and need to be added to service
                    inserts = []  # this will hold the new feature class globals in original case
                    for feature_class_global in feature_class_globals:
                        if feature_class_global.lower() not in set(fsg_translator):  # keys - lowercase comparison
                            inserts.append(feature_class_global)  # Append original case

                    # Remove inserts from feature_class_globals so we don't waste time checking if an update
                    for insert in inserts:
                        feature_class_globals.pop(insert)

                    # Add the new features
                    if inserts:
                        insert_where = buildWhereClauseFromList(feature_class_layer, 'GlobalID', inserts)
                        arcpy.SelectLayerByAttribute_management(feature_class_layer, where_clause=insert_where)
                        # Filter out identical geometries from existing selection.
                        # This guards against redundant offline syncs - globals change once appended to service, 
                        # so redundant use of the tool with the same source data will flag previously synced
                        # source globals as new when rerun - unless filtered out using identical geometry.
                        arcpy.management.SelectLayerByLocation(
                            feature_class_layer,
                            "ARE_IDENTICAL_TO",
                            feature_service_layer,
                            selection_type='REMOVE_FROM_SELECTION',
                            )
                        selected_inserts = arcpy.Describe(feature_class_layer).FIDSet  # ';' delimited string.. 
                        if selected_inserts:
                            msg = '{} Insert(s)..'.format(len(selected_inserts.split(';')))
                            insert_data = '{}_inserts'.format(feature_layer_name)
                            arcpy.CopyFeatures_management(feature_class_layer, os.path.join(temp_gdb, insert_data))
                            # Append the new records
                            if not debug_mode.value:
                                arcpy.Append_management(feature_class_layer, feature_service_layer, 'NO_TEST')
                        else:
                            msg = '0 Insert(s)..'
                        log_message(msg)


                    # UPDATES: These need to be removed from service and re-inserted
                    updates = []  # this will hold the update feature class globals in original case
                    for feature_class_global, last_edit_date in feature_class_globals.items():
                        # Datetimes - the past is < the present
                        # If Collector edit dates are more recent that service edit dates:
                        # Use lowercase fcg to get correct fsg case
                        if last_edit_date > feature_service_globals[fsg_translator[feature_class_global.lower()]]:
                            updates.append(feature_class_global)  # append original version
                    msg = '{} Update(s)..'.format(len(updates))
                    log_message(msg)

                    # Delete the rows that need to be updated
                    if updates:
                        # Copy the update data
                        update_data = '{}_updates'.format(feature_layer_name)
                        update_where = buildWhereClauseFromList(feature_class_layer, 'GlobalID', updates)
                        arcpy.SelectLayerByAttribute_management(feature_class_layer, where_clause=update_where)
                        arcpy.CopyFeatures_management(feature_class_layer, os.path.join(temp_gdb, update_data))
                        # Delete the records from the feature service that need updated
                        # Translate the fcg update to correct case fsg
                        fsg_updates = [fsg_translator[update.lower()] for update in updates]
                        delete_where = buildWhereClauseFromList(feature_service_layer, 'GlobalID', fsg_updates)
                        arcpy.SelectLayerByAttribute_management(feature_service_layer, where_clause=delete_where)
                        if not debug_mode.value:
                            # Remove the old data and add the new data
                            arcpy.DeleteRows_management(feature_service_layer)
                            arcpy.Append_management(feature_class_layer, feature_service_layer, 'NO_TEST')

                except:
                    logging.exception("Could not update {}..".format(feature_layer_name))

            log_message('Successful completion..')

        except:
            arcpy.AddError(traceback.format_exc())
            logging.exception("An error occurred..")

        finally:
            # Shut down the logger
            logger = logging.getLogger()
            handlers = logger.handlers[:]
            for handler in handlers:
                handler.close()
                logger.removeHandler(handler)

            arcpy.Delete_management(temp_xml)
            deleteInMemory()

        return
