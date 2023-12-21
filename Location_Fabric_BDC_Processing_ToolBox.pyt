# -*- coding: utf-8 -*-
#------------------------------------------------------------------------------

# toolbox metadata
__name__            = 'Location_Fabric_BDC_Processing_ToolBox'
__alias__           = 'Location Fabric And BDC Processing ToolBox'
__formatName__      = 'ArcToolbox Toolbox'
__author__          = 'ahamptonTIA'
__credits__         = [__author__]
__version__         = '0.0.8'
__maintainer__      = __author__
__email__           = 'https://github.com/ahamptonTIA'
__org__             = 'National Telecommunications and Information Administration (NTIA)'
__subOrg__          = 'Performance & Data Analytics'
__org_email__       = 'nbam@ntia.gov'
__orgGitHub__       = 'https://nbamgis.github.io/NTIA-Performance-and-Data-Analytics-on-GitHub'
__github_url__      = f'https://github.com/{__author__}/{__name__}'
__status__          = 'Beta'
__create_date__     = '20231011'  
__version_date__    = '20231221'
__ArcGISFormat__    = '1.0'
__searchKeys__      = ['BDC', 'Location Fabric', 'NTIA', 'FCC']
__idCreditStr__     = f'''<b>Point of Contact (POC):</b> {__subOrg__}
                          <b>Organization:</b> {__org__}
                          <b>Email:</b> <a href="mailto:{__org_email__}">{__org_email__}</a>
                          <b>Additional Credits:</b> pyt_meta project, a python toolbox metadata automation package.
                              See: <a href="https://github.com/GeoCodable/pyt_meta" target="_blank" STYLE="text-decoration:underline;">pyt_meta</a>
                        '''
__SyncOnce__        = 'TRUE'
__ArcGISProfile__   = 'ItemDescription'
__license__  = \
    """
    <b>Software Disclaimer/Release:</b>
        This software was developed by employees of the National Telecommunications
        and Information Administration (NTIA), an agency of the Federal Government
        and is provided to you as a public service. Pursuant to Title 15 United States
        Code Section 105, works of NTIA employees are not subject to copyright protection
        within the United States. The software is provided by NTIA 'AS IS.' NTIA MAKES NO
        WARRANTY OF ANY KIND, EXPRESS, IMPLIED OR STATUTORY, INCLUDING, WITHOUT LIMITATION,
        THE IMPLIED WARRANTY OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE,
        NON-INFRINGEMENT AND DATA ACCURACY. NTIA does not warrant or make any representations
        regarding the use of the software or the results thereof, including but not limited
        to the correctness, accuracy, reliability or usefulness of the software.
        To the extent that NTIA holds rights in countries other than the United States, you
        are hereby granted the non-exclusive irrevocable and unconditional right to print,
        publish, prepare derivative works and distribute the NTIA software, in any medium,
        or authorize others to do so on your behalf, on a royalty-free basis throughout the
        World. You may improve, modify, and create derivative works of the software or any
        portion of the software, and you may copy and distribute such modifications or works.
        Modified works should carry a notice stating that you changed the software and should
        note the date and nature of any such change. You are solely responsible for determining
        the appropriateness of using and distributing the software and you assume all risks
        associated with its use, including but not limited to the risks and costs of program
        errors, compliance with applicable laws, damage to or loss of data, programs or equipment,
        and the unavailability or interruption of operation. This software is not intended to be
        used in any situation where a failure could cause risk of injury or damage to property.
        Please provide appropriate acknowledgments of NTIA's creation of the software in any
        copies or derivative works of this software.
    """
__data_restrictions__ = \
    '''
    <b>Data Restrictions:</b>
        Prior to sharing results, please verify the output dataset fields/columns
        meet the distribution requirements in accordance with your organization’s signed
        license agreement with CostQuest Associates.
        See: <a href="https://help.bdc.fcc.gov/hc/en-us/articles/10419121200923-How-Entities-Can-Access-the-Location-Fabric-" target="_blank" STYLE="text-decoration:underline;">Why Do I Need a Fabric License?</a>          
    '''
__info__ = \
    '''
    Description:
        The Location Fabric BDC Processing Toolbox is a collection of 
        ArcGIS python tools which make processing large volume 
        Location Fabric and FCC BDC data more streamlined and capable
        within ArcGIS Pro.
    '''
__tools__ = [
        '''
        Creates a point layer or table representing the highest
        reported service levels defined by NTIA BEAD program as
        reliable technologies which include Copper Wire, Coaxial
        Cable/HFC, Optical Carrier/Fiber to the Premises,
        Licensed Terrestrial Fixed Wireless and, Licensed-by-Rule
        Terrestrial Fixed Wireless. 
        ''',
        '''
        Creates a point layer from a location fabric dataset using
        user specified fields for output.
        '''
        
    ]
#----------------------------------------------------------------------------


import os, sys, gc, re, time, requests, warnings
import json, tempfile, arcpy, inspect
import pandas as pd
import arcgis
from arcgis.features import GeoAccessor, GeoSeriesAccessor

import importlib.util
from pathlib import Path
from collections import OrderedDict
from datetime import datetime 
from xml.etree.ElementTree import \
     ElementTree as et_root, \
     Element as et_elm, \
     SubElement as et_se

#----------------------------------------------------------------------------

def clean_df_memory(df):
    """Clears and releases memory used by pandas dataframes
    using the garbage collector.

    Parameters:
    -----------
    df: pandas.DataFrame or a list of dataframes, required
        DataFrames to be removed from memory

    Returns:
    -------
        None
    """

    # Check input type: DataFrame or list of DataFrames
    if isinstance(df, pd.DataFrame):
        # Delete DataFrame object
        del df
    elif isinstance(df, list):
        # Loop through elements in list
        for d in df:
            # Check if element is a DataFrame
            if isinstance(d, pd.DataFrame):
                # Delete DataFrame object
                del d
    else:
        raise TypeError("Input must be a pandas DataFrame or a list of DataFrames")

    # Trigger garbage collection for memory release
    gc.collect()

#----------------------------------------------------------------------------
    
def get_iterative_filename(name, workspace):
    """Returns an iterative item name if the output
    filename already exists.
    Parameters:
    -----------
    name: str, required
        The input item or file name.
    workspace: str, required
        The output workspace path.
    Returns
    -------
        The iterative output filename as needed.
    """
    ext = None
    if len(os.path.splitext(name)) > 1:
        name, ext = os.path.splitext(name)
    exists = arcpy.Exists(os.path.join(workspace, name))

    if exists:
        # The output file already exists, generate an iterative filename.
        cnt = 1
        while exists:
            new_filename = f"{name}_{cnt}"
            valid_name = arcpy.ValidateTableName(new_filename, workspace)
            if not valid_name.endswith(f'_{cnt}'):
                valid_name = arcpy.ValidateTableName(valid_name[:(-1 * len(f'_{cnt}'))], workspace)
            exists = arcpy.Exists(os.path.join(workspace, valid_name))
            cnt += 1
        name = valid_name
    else:
        name = arcpy.ValidateTableName(name, workspace)

    if bool(ext):
        name = f'{name}{ext}'
    return name

#----------------------------------------------------------------------------
        
def get_default_gdb():
    """
    Gets the default geodatabase.
    Parameters:
    -----------
        None
    Returns
    -------
        gdb : str
            Path to the default geodatabase.
    """    
    try:
        aprx = arcpy.mp.ArcGISProject('CURRENT')
        gdb = aprx.defaultGeodatabase
    except:
        gdb = arcpy.env.workspace
    return gdb

#----------------------------------------------------------------------------
        
def get_default_dir():
    """
    Gets the default workspace folder.
    Parameters:
    -----------
      None
    Returns
    -------
        _dir : str
            Path to the default folder.
    """    
    try:
        aprx = arcpy.mp.ArcGISProject('CURRENT')
        _dir = aprx.homeFolder
    except:
        _dir = tempfile.gettempdir()
    return _dir

#----------------------------------------------------------------------------

def get_default_output_name(parameters=[], workspace=None):
    """
    Gets the file names of two parameters and concatenates the
    string values to return a valid feature class name given the
    two input file names. 
    Parameters:
    -----------
        parameters: (list of str)
            List of strings to concat
        workspace: (str)
            The workspace path 
    Returns:
    -----------
        A string containing the validated default feature or table
        name based on two input file names.
    """

    if len(parameters) > 1:
        s = '_'.join(parameters)
    else:
        s = parameters[0]
    if not bool(workspace):
        workspace = get_default_gdb()
        
    vn = arcpy.ValidateTableName(s,workspace)  
    return vn

#----------------------------------------------------------------------------

def get_state_fabric_versions(csv):
    """
    Gets a list of dictionaries where the unique values
    combinations of two columns in a Pandas dataframe
    are returned.
    Parameters:
    -----------
        dataframe (pandas.DataFrame):
            Pandas dataframe.
        field_names (list[str]): 
            A list of field names.
    Returns:
    -----------
        list[dict]: A list of dictionaries
            List of dictionaries where the unique
            values combinations of the two columns are returned.
                ex. [{'state_fips': '46', 'fcc_rel': 12222022}]
    """

    u_vals = []
    
    chunksize = 20000

    with pd.read_csv(csv, dtype=str, chunksize=chunksize) as reader:
        for c in reader:
            c['block_geoid'] = c['block_geoid'].str.zfill(15)
            c['state_fips'] = [str(x)[:2] for x in c['block_geoid']]

            uv_df = c.groupby(['state_fips','fcc_rel']).size().reset_index()
            c_uv = uv_df[['state_fips','fcc_rel']].to_dict('records')

            for d in c_uv:
                d['fcc_rel'] = datetime.strptime(
                                    str(d['fcc_rel']).zfill(8), '%m%d%Y')
                
                if d not in u_vals:
                    u_vals.append(d)
    # clear up memory
    clean_df_memory([])
    return u_vals

#----------------------------------------------------------------------------

def get_nearest_date(date_object, list_of_dates, max_offset=None):
    """Finds the date in the list of dates that
    is nearest to the given date object.
    Parameters
    ----------
        date_object : datetime.datetime object, required
            A datetime.datetime object.
        list_of_dates : list, required
            A list of datetime.datetime objects.
        max_offset : int, optional
            The max off set in days.  This is the range of
            days to look for a version of the fabric.
            If no version dates fall within this range,
            none will be returned
    Returns
    -------
        A datetime.datetime object representing the closest date in the list of dates.
    """

    closest_date = None
    closest_date_delta = None

    for date in sorted(list_of_dates):
        delta = abs(date - date_object)
        if closest_date_delta is None or delta < closest_date_delta:
            closest_date = date
            closest_date_delta = delta

    if max_offset:
        delta = abs(date_object - closest_date)
        if delta.days > max_offset:
            arcpy.AddWarning('No fabric version/dates found within specified range!') 
            return None

    return closest_date

#----------------------------------------------------------------------------

def request_handler(url, headers=None, ip_session=None, debug=True):
    """
    Function acts as a request handler to print response status 
    and error messages. Errors will be printed and any errors will
    stop the remaining script/tool from executing.
    
    *See the requests python docs for more keyword argument options.
    
    Parameters
    ----------
        url : str, required
            REST API url for the ESRI Feature Service
        headers: dict, optional
            Dictionary of request headers
        ip_session: obj, optional
            Existing input requests session object
            Default, new session will be created
        debug: Boolean, optional
            Boolean option to print messages;
            Default, True, all messages will be printed
    Returns
    -------
    rtn_tuple : tuple 
        rtn_tuple:  tuple containing the response object (resp), 
                    the status category (status), and a more detailed 
                    message to convey response results (message)    
    Example
    -------
    >>> url = "https://website.com/..."
    >>> response, status, message = _request_handler(
                                                    url=url, 
                                                    log=log, 
                                                    debug=True,
                                                    )   
    >>> print(status)
    >>> 'Success' 
    >>> print(message)
    >>> 'Size: 529744(B),Time :1.140771(s)'                                     
    """
       
    resp    = None
    status  = ''
    message = ''
    elapsed_time = '0 (ms)'
    size_bytes = 0
    
    if not ip_session:
        session=requests.session()
    else:
        session = ip_session
    try:   
        resp = session.get(url, headers=headers)
        resp.raise_for_status()
        
    except requests.exceptions.HTTPError as err:
        status  = 'Error: (HTTPError)'
        message = err
        arcpy.AddError(f'{status} : {message}')
    except requests.exceptions.ConnectionError as err:
        status  = 'Error: (ConnectionError)'
        message = err
        arcpy.AddError(f'{status} : {message}')
    except requests.exceptions.Timeout as err:
        status  = 'Error: (Timeout)'
        message = err
        arcpy.AddError(f'{status} : {message}')
    except requests.exceptions.RequestException as err:
        status  = 'Error: (Unidentified)'
        message = err
        arcpy.AddError(f'{status} : {message}')
    finally:
        if bool(resp):
            if resp.status_code != 200 or 'error' in (resp.text).lower():
                status   = 'Error: (Unidentified)'
                message  = f'ResponseText:{resp.text}'
                arcpy.AddError(f'{status} : {message}')
            else:    
                status   = 'Success'
                message  = [f'Size: {len(resp.content)}(B),' 
                           f'Time :{resp.elapsed.total_seconds()}(s)']
                
            elapsed_time = f'{resp.elapsed.total_seconds()*1000}(ms)'
            size_bytes = f'{len(resp.content)}(B)'
    if debug:
        arcpy.AddMessage(f'\t-{status} |:| {message}')
    
    rtn_tuple = (resp, status, message)
    if not ip_session:
        session.close()
    return rtn_tuple    

#----------------------------------------------------------------------------

def download_zip(url, out_path, headers=None, session=None, chunk_size=128, debug=False):
    """
    Function downloads zip files from a url
    
    *See the requests python docs for more keyword argument options.
    
    Parameters
    ----------
        url : str, required
            URL to the zip file
        out_path : str, required
            Output file path for the zip file        
        headers: dict, optional
            Dictionary of request headers
        session: obj, optional
            Existing input requests session object
            Default, new session will be created
        chunk_size: int, optional
            Max data size per chunk/stream
            Default, 128
        debug : Boolean, optional
            Option to print all messages        
    Returns
    -------
        None                                   
    """    


    try:
        if debug:
            arcpy.AddMessage(f'\t-Requesting data from: {url}')
        if session:
            r = session.get(url, headers=headers, stream=True)
        else:
            r = requests.get(url, headers=headers, stream=True)
        with open(out_path, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=chunk_size):
                fd.write(chunk)
    except requests.exceptions.RequestException as err:
        arcpy.AddError(f'Failed to download zip: {url} - Error: {err}')
    except Exception as err:
        arcpy.AddError(f'Unexpected error during download: {err}')
    finally:
        if session is None:
            r.close()

#----------------------------------------------------------------------------

def get_fcc_bdc_data(fabric_version, state, tech_codes=None,
                     download_dir=None, _session=None, debug=False):
    """
    Function downloads the FCC BDC data the FCC Braodband map website  
    Parameters
    ----------
        fabric_version : str, required
            Date string of the farbic being used
        state : int, required
            State fips code
        download_dir : str, optional
            Path to directory for zipfile downloads.
            Defaults to the ArcGIS pro project or tempfile folders
        tech_codes : list of int's or string tech codes
            List of FCC BDC tech codes (int) to download. 
        _session : requests.session object, optional
            requests session object.
        debug : Boolean, optional
            Option to print all messages
    Returns
    -------
        tuple
            tuple of location fabric date, BDC update date, list of zip file paths                                   
    """

    try:
        zips = []

        if not bool(tech_codes):
            tech_codes = [10,40,50,71,72]
        else:
            tech_codes = [int(x) for x in tech_codes]
        if not bool(download_dir):
            download_dir = os.path.join(get_default_dir(), f"bdc_data")
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
            
        # request params:
        # create a requests session 
        if not _session:
            _session = requests.session()
        # set the default headers for the FCC Broadband Map Download homepage
        headers =   {
                    "accept": "text/html",
                    "User-Agent": "ArcGIS Python - Location_Fabric_BDC_Processing_ToolBox.pyt",
                    }
        # set the base URL to the FCC Broadband Map Download API
        api_base_url = r'https://broadbandmap.fcc.gov/nbm/map/api'
        # set the URL to the FCC Broadband Map Download API filing data request
        filing_url = f'{api_base_url}/published/filing'
        # set the base URL to the FCC Broadband Map Download API map_processing_updates data request
        map_processing_base_url = 'https://broadbandmap.fcc.gov/api/reference/map_processing_updates'
        # set the base URL to the FCC Broadband Map Download API data request
        nbm_data_base_url = f'{api_base_url}/national_map_process/nbm_get_data_download'
        # set the base URL to getNBMDataDownloadFile
        dl_url_base = f'{api_base_url}/getNBMDataDownloadFile'
        # set the numeric value for the file links (unknown if or how this changes)
        file_numb_id = 1

        # request a json object of fabric dates and process_uuid's
        response, status, message = request_handler(
                                                    filing_url, 
                                                    headers=headers, 
                                                    ip_session=_session, 
                                                    debug=debug
                                                    )
        
        # get the process uuid based on the location fabric date
        filing_dict = {datetime.strptime(l['filing_subtype'], "%B %d, %Y"):
                    l['process_uuid']
                    for l in response.json()['data']}

        fabric_date = get_nearest_date(date_object=fabric_version,
                                       list_of_dates=list(filing_dict.keys()),
                                       max_offset=90)
        
        if not fabric_date:
            raise InvalidFabric(fabric_version)
            
        p_uuid = filing_dict[fabric_date]


        # get the BDC update date:
        # set the dynamic URL to the FCC Broadband Map Download API map_processing_updates data request
        map_processing_updates_url = f'{map_processing_base_url}/{p_uuid}'
        # request all html text from the FCC Broadband Map Download API
        response, status, message = request_handler(
                                                    map_processing_updates_url, 
                                                    headers=headers, 
                                                    ip_session=_session, 
                                                    debug=debug
                                                    )

                                   
        bdc_date = datetime.strptime(response.json()['data'][0]['last_updated_date'],
                                              "%Y-%m-%dT%H:%M:%S.%fZ"
                                              )

        fabric_date_str = fabric_version.strftime("%Y%m%d")
        bdc_update_date_str = bdc_date.strftime("%Y%m%d")
        
        arcpy.AddMessage(f'BDC data current as of: {bdc_date.date().isoformat()}')
        
        # get a list of files:
        # set the URL to the FCC Broadband Map Download API data request
        cur_data_url = f'{nbm_data_base_url}/{p_uuid}'
        # request a list of available file downloads from the FCC Broadband Map Download API
        response, status, message = request_handler(
                                                    cur_data_url, 
                                                    headers=headers, 
                                                    ip_session=_session, 
                                                    debug=debug
                                                    )
        # filter the files required
        full_file_set = response.json()['data']
        
        filtered_file_set = [i for i in full_file_set 
                                if i['download_available'] == 'Yes'
                                and bool(i['state_fips'])
                                and i['state_fips'] == state
                                and i['data_category'] =='Nationwide'
                                and bool(i['technology_code'])
                                and (i['technology_code']).isnumeric() 
                                and int(i['technology_code']) in tech_codes
                            ]

        # download the zip files
        iter_cnt = 1
        for i in filtered_file_set:
            url = f'{dl_url_base}/{i["id"]}/{file_numb_id}'
            arcpy.AddMessage(f'Downloading: ({iter_cnt}) of {(len(filtered_file_set))} for state fips:{state}')
            arcpy.AddMessage(f'\t-{i["file_name"]}')
            zip_path = os.path.join(download_dir, f'{i["file_name"]}.csv.zip' )
            download_zip(url,
                         zip_path,
                         headers=headers,
                         session=_session,
                         chunk_size=128,
                         debug=debug
                         )
            zips.append(zip_path)
            iter_cnt += 1
        arcpy.AddMessage(f'Total files downloaded: {len(zips)} for state fips:{state}')

        
        return (fabric_date_str, bdc_update_date_str, zips)

    except InvalidFabric:
        arcpy.AddError('''The input location fabric file is invlaid\n
        FCC BDC reporting data is not available yet for the given Location Fabric version!''')
        sys.exit(0)
        return(None, None, None)
        
    except:
        # Catch any other default errors 
        e = sys.exc_info()[1]
        arcpy.AddError(e)
#----------------------------------------------------------------------------

def read_bdc_data(zipped_csv_files, tech_codes):
    """
    Takes in a list of zipped BDC CSV files and filters the data
    by technology code to retrun a single dataframe of filtered data.
    Parameters
    ----------
        zipped_csv_files : list of str's, required
            List of file paths to zipped FCC BDC CSV files.
        tech_codes : list of int's or string tech codes
            List of FCC BDC tech codes (int) to download. 
    Returns
    -------
        bdc_data_df : pandas.DataFrame
            A single concat dataframe of filtered BDC data                                 
    """
    if not bool(tech_codes):
        tech_codes = [10,40,50,71,72]
    else:
        tech_codes = [int(x) for x in tech_codes]
    # Create a list of bdc columns to read.
    # By not reading all available columns, memory usage is optimized
    use_columns = [
                    'location_id',
                    'technology',
                    'max_advertised_download_speed',
                    'max_advertised_upload_speed',
                    'low_latency',
                  ]

    # Set the BDC columns to treat as integers
    int_cols = [
                'technology', 'max_advertised_download_speed', 
                'max_advertised_upload_speed', 'low_latency'
               ]

    # Create a list to store DataFrames
    dataframes = []

    # Iterate over the list of zipped CSV files
    for zipped_file in zipped_csv_files:

        # Read the zipped BDC CSV file into a DataFrame
        _df = pd.read_csv(zipped_file, 
                          compression="zip", 
                          dtype=str,
                          usecols=use_columns
                         )
       
        # Set columns to treat as integers
        _df[int_cols] = _df[int_cols].astype(int)

        # Filter by specified technology codes 
        _df = _df[_df['technology'].isin(tech_codes)]   
        
        # Add the DataFrame to the list of DataFrames
        if len(_df) > 0:
            dataframes.append(_df)

    if len(dataframes) > 0:
        if len(dataframes) > 1:
            # Concatenate the list of BDC DataFrames into a single BDC DataFrame
            bdc_data_df = pd.concat(dataframes)
        elif len(dataframes) == 1:
            bdc_data_df = dataframes[0].copy(deep=True)
    
        # Clear initial file read dataframes to conserve resources/memeory
        clean_df_memory(dataframes)

        # Print a count of BDC records 
        arcpy.AddMessage(f"Relevant BDC record count:  {len(bdc_data_df):,}")
        return bdc_data_df
    else:
        arcpy.AddWarning(f"No BDC records found!")
        return None
#----------------------------------------------------------------------------

def read_location_fabric(loc_fab_path, columns=None, bsl_flag=True):
    """
    Takes in a location fabric path and filters the data
    by BSL flag and returns only the required columns. 
    Parameters
    ----------
        loc_fab_path : str, required
            Paths to a location farbic CSV file.
        tech_codes : list of int's
            List of FCC BDC tech codes (int) to download.
        use_columns : list
            List of columns to return/read
        bsl_flag: Boolean
            True false to return only bsl True records
    Returns
    -------
        location_fabric_df : pandas.DataFrame
            A single dataframe of filtered location fabric data                                 
    """    
    # Create a list of columns to read.
    # By not reading all columns, memory is optimized
    if not bool(columns):
        columns = [
                    'location_id',
                    'block_geoid',
                    'latitude',
                    'longitude',
                    'bsl_flag',
                    'fcc_rel'
                  ]
    read_columns = columns

    if bsl_flag and 'bsl_flag' not in columns:
        read_columns = columns + ['bsl_flag']
            
    # Read the un-zipped Location Fabric CSV file into a DataFrame
    location_fabric_df = pd.read_csv(
                             loc_fab_path, 
                             dtype=str,
                             usecols=read_columns
                             )

    # Filter to ensure only BSL's are included in the analysis
    if bsl_flag: 
        location_fabric_df = location_fabric_df[
                            location_fabric_df['bsl_flag'].str.lower()
                            =='true']   

    location_fabric_df = location_fabric_df[columns]
    # Print a count  of BSL records
    arcpy.AddMessage(f"Total BSL's:  {len(location_fabric_df):,}")
    return location_fabric_df

#----------------------------------------------------------------------------

def calculate_service_level(row):
    """Function calculates the service level based on service level criteria.
    The enumerated service level is calculated for a given row in a pandas 
    dataframe/series. An enumerated service level rank value is returned for 
    a row of BDC data where; -1= 'Service Level Error', 0= 'Unserved', 
    1= 'Underserved',and  2= 'Served'
    
    Parameters
    ----------
        pd.Series:
            A Pandas row of data containing the columns:
                -'max_advertised_download_speed'
                -'max_advertised_upload_speed'
                -'low_latency'
    Returns
    -------
        int: 
            An enumerated service level calculation results:
                -1: 'Service Level Error'
                 0: 'Unserved' 
                 1: 'Underserved'
                 2: 'Served'

    Example:
        >>> df['enum_max_service_level'] = df.apply(calculate_service_level, axis=1)
    """
    #------------------------------------------------------------------------    
    # Check if download or upload speed values are missing
    if (
        pd.isna(row['max_advertised_download_speed'])
        or pd.isna(row['max_advertised_upload_speed'])
        or pd.isna(row['low_latency'])
    ):
        return 0  # If speed values are missing, label as (0) "Unserved" 
    #------------------------------------------------------------------------
    # Check if the location has low latency
    elif row['low_latency'] == 0:
        return 0  # If Low latency is False (0), label as (0) "Unserved"      
    #------------------------------------------------------------------------    
    # Check download and upload speed conditions for "Unserved" category
    elif (
        row['max_advertised_download_speed'] < 25
        or row['max_advertised_upload_speed'] < 3
    ):
        return 0  # If speeds are below 25/3, label as (0) "Unserved"
    #------------------------------------------------------------------------  
    # Check download and upload speed conditions for "Underserved" category
    elif (
        25 <= row['max_advertised_download_speed'] < 100
        or 3 <= row['max_advertised_upload_speed'] < 20
    ):
        return 1  # If speeds are at or above 25/3, but less than 100/20, label as (1) "Underserved"
    #------------------------------------------------------------------------   
    # Check download and upload speed conditions for "Served" category
    elif (
        row['max_advertised_download_speed'] >= 100
        or row['max_advertised_upload_speed'] >= 20
    ):
        return 2  # If speeds are equal to or above 100/20, label as (2) "Served"
    #------------------------------------------------------------------------
    # with none of the criteria met, label with an (-1) "Service Level Error" 
    else:
        return -1      

#----------------------------------------------------------------------------
def get_bsl_max_service_levels(location_fabric_df, bdc_data_df):
    """Function takes in the locatoin fabric and bdc data as
    pandas dataframes and calls calculate_service_level() to 
    calculate the max service level per location based on 25/3/1 
    service level criteria.
    The enumerated service level is calculated for a given location
    in the fabric pandas dataframe/series. An enumerated service level rank 
    value is returned with text column named 'max_service_level' 
    for the service level category where; -1= 'Service Level Error', 
    0= 'Unserved', 1= 'Underserved', 2= 'Served'
    Parameters
    ----------
        pd.Series:
            A Pandas row of data containing the columns:
                -'max_advertised_download_speed'
                -'max_advertised_upload_speed'
                -'low_latency'
    Returns
    -------
        int: 
            An enumerated service level calculation results:
                -1: 'Service Level Error'
                 0: 'Unserved' 
                 1: 'Underserved'
                 2: 'Served'

    Example:
        >>> df['enum_max_service_level'] = df.apply(calculate_service_level, axis=1)
    """

    arcpy.AddMessage('Determining max service levels by location')
    # Some BSL's may not have a service level reported within the BDC data
    # Left join the Location Fabric BSL to the BDC dataset, Nulls will be labled as "Unserved"
    df = location_fabric_df.merge(bdc_data_df, on='location_id', how='left', suffixes=('', '_df2_'))

    # Remove columns duplicated in the join
    dup_cols = [c for c in df.columns if c.endswith('_df2_')]
    df = df.drop(columns=dup_cols)

    # Clear up some memmory by removing the BDC and location fabric dataframes
    clean_df_memory([bdc_data_df])

    # Apply the calculate_service_level function to each row in the DataFrame
    # This creates a new column called "enum_max_service_level" 
    df['enum_max_service_level'] = df.apply(calculate_service_level, axis=1)

    # Group by each BSL to obtain the highest reported service level
    # This determines the max service level for each BSL
    bsl_max_srvc_lvls_df = df.groupby(['location_id']
                                     )['enum_max_service_level'].max().reset_index()
    clean_df_memory([df])
    # Map the service level labels for each BSL
    svc_lvl_map = {-1: 'Service Level Error', 0: 'Unserved', 1: 'Underserved', 2: 'Served'}

    bsl_max_srvc_lvls_df['max_service_level'] = bsl_max_srvc_lvls_df['enum_max_service_level'].map(svc_lvl_map)

    # Print a count  of max service level records for the state
    arcpy.AddMessage(f"Total max service level records:  {len(bsl_max_srvc_lvls_df):,}")
    
    return(bsl_max_srvc_lvls_df)

#----------------------------------------------------------------------------
# metadata

# metadata constants
# ---------------------------------------------------------------------------

# constant for the xml root default attributes 
DEFAULT_XML_ATTRIBS= \
    {
        "metadata": {"xml:lang": "en"},
        "ScopeCd": {"value": "005"},
        "mdDateSt": {"Sync": "TRUE"}
    }

# constant for the default toolbox xml structure 
DEFAULT_TOOLBOX_XML_STRUCT = \
    OrderedDict(
        {
            "metadata": "None",
            "Esri": "metadata",
            "CreaDate": "Esri",
            "CreaTime": "Esri",
            "ArcGISFormat": "Esri",
            "SyncOnce": "Esri",
            "ModDate": "Esri",
            "ModTime": "Esri",
            "scaleRange": "Esri",
            "minScale": "scaleRange",
            "maxScale": "scaleRange",
            "ArcGISProfile": "Esri",
            "toolbox": "metadata",
            "arcToolboxHelpPath": "toolbox",
            "dataIdInfo": "metadata",
            "idCitation": "dataIdInfo",
            "resTitle": "idCitation",
            "idPurp": "dataIdInfo",
            "searchKeys": "dataIdInfo",
            "idAbs": "dataIdInfo",
            "idCredit": "dataIdInfo",
            "resConst": "dataIdInfo",
            "Consts": "resConst",
            "useLimit": "Consts",
            "distInfo": "metadata",
            "distributor": "distInfo",
            "distorFormat": "distributor",
            "formatName": "distorFormat",
            "mdHrLv": "metadata",
            "ScopeCd": "mdHrLv",
            "mdDateSt": "metadata",
        }
    )

# constant for the default tool xml structure 
DEFAULT_TOOL_XML_STRUCT = \
    OrderedDict(
        {
            "metadata": "None",
            "Esri": "metadata",
            "CreaDate": "Esri",
            "CreaTime": "Esri",
            "ArcGISFormat": "Esri",
            "SyncOnce": "Esri",
            "ModDate": "Esri",
            "ModTime": "Esri",
            "scaleRange": "Esri",
            "minScale": "scaleRange",
            "maxScale": "scaleRange",
            "tool": "metadata",
            "arcToolboxHelpPath": "tool",
            "summary": "tool",
            "usage": "tool",
            "scriptExamples": "tool",
            "parameters": "tool",
            "dataIdInfo": "metadata",
            "idCitation": "dataIdInfo",
            "resTitle": "idCitation",
            "idCredit": "dataIdInfo",
            "searchKeys": "dataIdInfo",
            "resConst": "dataIdInfo",
            "Consts": "resConst",
            "useLimit": "Consts",
            "distInfo": "metadata",
            "distributor": "distInfo",
            "distorFormat": "distributor",
            "formatName": "distorFormat",
            "mdHrLv": "metadata",
            "ScopeCd": "mdHrLv",
            "mdDateSt": "metadata",
        }
    )

# constants for default Esri Toolbox metadata values
TOOLBOX_FORMAT_NAME = "ArcToolbox Toolbox"
ARC_GIS_FORMAT = "1.0"
SYNC_ONCE = "TRUE"
MIN_SCALE = "150000000"
MAX_SCALE = "5000"
ARC_GIS_PROFILE = "ItemDescription"

# constants for default Esri Tool metadata values
TOOL_FORMAT_NAME = "ArcToolbox Tool"

# ---------------------------------------------------------------------------


def import_toolbox(toolbox_path):
    """Function imports an ArcGIS python toolbox by name
    from the current script directory and returns
    the Toolbox class object.
    :param - toolbox_path - python toolbox path
    :returns -pyt.Toolbox - ArcGIS python Toolbox class object"""
    if toolbox_path.endswith('.pyt'):
        importlib.machinery.SOURCE_SUFFIXES.append('pyt')
    toolbox_name = os.path.splitext(
        os.path.basename(toolbox_path))[0]    
    spec = importlib.util.spec_from_file_location(
         toolbox_name, toolbox_path)
    pyt = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pyt)
    return pyt

# ---------------------------------------------------------------------------


def get_file_dates(file_path, date_fmt ='%Y%m%d', time_fmt='%H%M%S%S'):
    '''Function returns created, modified, and last accessed
    date and time values along with the last modified date.
    and time for a given file in UTC.
    :param - file_path  - path to a file
    :param - date_fmt  - desired string date format
    :param - time_fmt  - desired string time format
    :returns - dt_vals - a tuple of file dates and times, see example
            ex. ( created date,created time,
                modified date, modified time,
                last accessed date, last accessed time,
                current date, current time)
    '''
    gt = time.gmtime
    # get the last modified date time
    mtime = os.path.getmtime(file_path)
    mod_date = \
             time.strftime(date_fmt, gt(mtime))
    mod_time = \
             time.strftime(time_fmt, gt(mtime)) 

    # get the created date time
    ctime = os.path.getctime(file_path)
    create_date = \
                time.strftime(date_fmt, gt(ctime))
    create_time = \
        time.strftime(time_fmt, gt(ctime)) 

    # get the last accessed date time
    atime = os.path.getctime(file_path)
    access_date = \
                time.strftime(date_fmt, gt(atime))
    access_time = \
        time.strftime(time_fmt, gt(atime)) 

    # get the current accessed date time
    cur_time = datetime.utcnow()
    cur_date = cur_time.strftime(date_fmt)
    cur_time = cur_time.strftime(time_fmt)

    dt_vals = (create_date, create_time,
                       mod_date, mod_time,
                       access_date, access_time,
                       cur_date, cur_time)
    return dt_vals


# ---------------------------------------------------------------------------


def py_text_to_html(str_value):
    """Function returns a multiline string variable from python
    as a html block. The inspect.cleandoc method is used to
    clean up indentation from python docstrings that are
    indented to line up within blocks of python code. All
    leading whitespaces are removed from the first line.
    Any leading whitespace that can be uniformly removed
    from the second line onwards are removed. Empty lines
    at the beginning and end are subsequently removed. Also,
    all tabs are expanded to spaces.
    :param - str_value  - string (multiline string)
    :returns -rtn_text- reformatted string value"""
    rtn_text = str_value
    rtn_text = inspect.cleandoc(str_value)
    rtn_text = "<br></br>".join(
        ["<span>{0}</span>".format(l) for l in rtn_text.splitlines()]
        )
    return rtn_text


# ---------------------------------------------------------------------------


def set_default_keywords(strs=[]):
    """Function returns search keywords from
    a list of strings.
    :param - strs  - list of string values to build keywords from
    :returns - fmt_kws - list object containing keywords"""
    kws = [kw for kw in re.split("[^a-zA-Z]", " ".join(strs)) if kw.isalnum()]
    kws = [(kw if any(e.isupper() for e in kw) else kw.upper()) for kw in kws]
    kws = list(set(kws))
    fmt_kws = []
    for t in kws:
        matches = re.findall(t, str(kws), re.IGNORECASE)
        if len(matches) > 1:
            for m in matches:
                if m != m.upper():
                    fmt_kws.append(m)
        if t.upper() not in [x.upper() for x in fmt_kws]:
            fmt_kws.append(t)
    fmt_kws = list(set(fmt_kws))
    return fmt_kws


# ---------------------------------------------------------------------------


def create_credits_list(user_name="", org_name="", email=""):
    """Function returns the ArcGIS portal user
    name , org, and email info (if logged in) as a list.
    Otherwise, just  the system user name will be
    returned.
    :param - user_name  - default user name
    :param - org_name  - default org name
    :param - email  - default email
    :returns - user_info - list object"""

    if user_name == "":
        try:
            import getpass
            userId = getpass.getuser()
        except:
            userId == "Unknown"
        
    try:
        if user_name == "" or org_name == "" or email == "":
            import arcpy

            active_portal = arcpy.GetActivePortalURL()
            portal_info = arcpy.GetPortalDescription(active_portal)
            # get the user name
            if user_name == "":
                userId = portal_info["user"]["fullName"]
            else:
                userId == user_name
                # get the org name
            if org_name == "":
                org_name = portal_info["name"]
                # get the user's email
            if email == "":
                email = portal_info["user"]["email"]
    except BaseException:
        # create the output json object
        pass
    user_info = [
                                "<b> Point of Contact (POC):{0}</b> ".format(userId),
                                "<b> Organization: {0}</b> ".format(org_name),
                                "<b> Email: {0}</b> ".format(email)
                          ]
    return user_info


# ---------------------------------------------------------------------------


def get_class_attrib(co, prop_name, default_value="",
                         attrib_multiline=True, dflt_multiline=True):
    """Function retrieves an object property value
    if it exists. Otherwise the default value is returned.
    Options for format the return value as an html block
    and/or remove multiline spacing are available.
    :param - co - input class/object
    :param - prop_name  - object property by name
    :param - default_value  - default for non-existent property
    :param - attrib_multiline - option to format class attribute as multiline text
    :param - dflt_multiline - option to format default value as multiline text"""

    if hasattr(co, prop_name):
        val = getattr(co, prop_name)
        if isinstance(val, str):
            if inspect.cleandoc(val) != val and attrib_multiline:
                val = py_text_to_html(val)
    else:
        val = default_value
        if isinstance(default_value, str):
            if inspect.cleandoc(default_value) != default_value and dflt_multiline:
                val = py_text_to_html(default_value)
    return val


# ---------------------------------------------------------------------------


def build_metadata_structure(metadata_dict, xml_attrib_dict):
    """Function generates a metadata xml etree structure
    given a ordered dictionary object representing
    the hierarchical structure/node order. See example below:
            ex. { 'root_elem': 'None',
                    'child_elem':'root_elem' }
    :param - metadata_dict    - Ordered dictionary; see example above
    returns - (xml_root, xml_elms)- a tuple containing the root xml element
                and a dictionary of all xml elements"""

    xml_elms = {}
    xml_root = None
    for elm, elm_par in metadata_dict.items():
        if str(elm_par) == "None":
            # make the root element
            xml_elms[elm] = et_elm(elm)
            if elm in xml_attrib_dict.keys():
                for attrib, val in xml_attrib_dict[elm].items():
                    xml_elms[elm].set(attrib, val)
            xml_root = xml_elms[elm]
            continue
        else:
            # make the sub element structures
            xml_elms[elm] = et_se(xml_elms[elm_par], elm)
            if elm in xml_attrib_dict.keys():
                for attrib, val in xml_attrib_dict[elm].items():
                    xml_elms[elm].set(attrib, val)
    return (xml_root, xml_elms)


# ---------------------------------------------------------------------------


def set_xml_text_by_class_attrib(class_inst, xml_elms, overwrite=False):
    """Function sets the text value for each xml element in
    a dictionary of xml elements when there is a
    corresponding  xml tag and class attribute in
    the class instance.
    :param - class_inst  - instance of a python object/class
    :param - xml_elms  - dictionary of xml etree elements
                                            ex. {xml_tag: etree_element}
    :param - overwrite  - allow existing xml text values to
                                            be overwritten
    :returns - xml_elms - dictionary of xml etree elements"""
    for tag, elm in xml_elms.items():
        class_attrib = get_class_attrib(class_inst, tag, "")
        if isinstance(class_attrib, str):
            if bool(class_attrib):
                if not elm.text or not bool(elm.text):
                    elm.text = class_attrib
                elif overwrite:
                    elm.text = class_attrib
    return xml_elms


# ---------------------------------------------------------------------------


class toolMetadata(object):
    def __init__(self, tb_metadata, tb_tool):
        self.tb_meta = tb_metadata
        self.tb_tool = tb_tool

        # get general tool information
        # ------------------------------------------------------------------------
        # get the tool name
        self.tool_name = self.tb_tool.__name__
        # get the xml path
        tb_pardir = os.path.abspath(os.path.join(
            self.tb_meta.toolbox_path,
            os.pardir))
        
        self.xml_path =  os.path.abspath(
            r'{0}\{1}.{2}.pyt.xml'.format(
                tb_pardir,
                self.tb_meta.toolbox_name,
                self.tool_name
                )
            )
        
        # create an instance of the tool
        self.tool_inst = self.tb_tool()

        # get tool level metadata from tool instance attributes
        # ------------------------------------------------------------------------
        # tool label
        self.label = get_class_attrib(self.tool_inst, "label", self.tool_name)
        # toolbox alias
        self.alias = self.tb_meta.alias
        # toolbox keywords
        self.toolbox_keywords = " ".join(self.tb_meta.keywords)
        # tool category
        self.category = get_class_attrib(self.tool_inst, "category", "Uncategorized")
        # summary/description text
        dflt_summary = """{0} is an ArcGIS python toolbox tool.
                                              Contact POC below for more information.""".format(
                                              self.tool_name)
        self.summary = get_class_attrib(self.tool_inst, "description", dflt_summary)
        # tool usage text
        self.usage = get_class_attrib(self.tool_inst, "usage", self.summary)
        # set derived tool metadata variables
        self.resTitle = "{0}.({1})".format(self.alias, self.category)
        
        # check for tool searchKeys in the tool attributes
        self.keywords = get_class_attrib(self.tool_inst, "searchKeys", [])
        # generate default keywords from the toolbox
        if not isinstance(self.keywords, list) or not bool(self.keywords):
            self.keywords = set_default_keywords(
                [self.label, self.tool_name, self.toolbox_keywords, self.category]
                )
        
        self.tb_meta.keyword_master.extend(self.keywords)
        # generate a default code sample from the toolbox and tool
        self.code_ex = get_class_attrib(self.tool_inst, "scriptExamples", {})
        self.code_ex = self.validate_code_examples()

    # set default Esri tool metadata values:
    # ---------------------------------------------------------------------------
    formatName = TOOL_FORMAT_NAME
    # --------------------------------------------------------------------------

    def gen_default_code(self):
        """Function generates a default code sample for an arcGIS tool.
        :param - self - arcGIS toolbox metadata object/class
        :returns - code_text- a text code sample"""

        param_text = ""

        tb_import = [
            "# import the toolbox as a module",
            "import arcpy",
            "arcpy.ImportToolbox(r'{0}',".format(
                self.tb_meta.toolbox_path),
            "{0}r'{1}')".format(" " * 20, self.tb_meta.alias),
            ]

        tool_call = "result = arcpy.{0}_{1}".format(
            self.tool_name, self.tb_meta.alias)
        lead_sp = " " * 11
        if hasattr(self.tool_inst, "getParameterInfo"):
            t_args = self.tool_inst.getParameterInfo()
            max_p_len = max([len(a.name) for a in t_args])
            p_sep = ",\n" + lead_sp
            param_text = p_sep.join(
                    [
                    "{0}{1}#{2}- Type({3})".format(
                        a.name,
                        " " * (max_p_len + 4 - len(a.name)),
                        a.displayName,
                        a.datatype)
                    for a in t_args
                    ]
                )
        tool_code = [
            "# call the tool and return the output",
            "{0}(\n{1}{2}\n{1})".format(tool_call, lead_sp, param_text)
            ]
        code_text = '{0}\n\n{1}'.format(
                                                        '\n'.join(tb_import),
                                                        '\n'.join(tool_code)
                                                        )
        return code_text

    # --------------------------------------------------------------------------

    def validate_code_examples(self):
        """Function to parse and validate code description json objects.
        See example json properly formatted object below:
        Ex. JSON object
            {'Code Sample 1' : {
                       'para' : 'Sample of tool code in python:',
                       'code' : ['def hello_world_func():',
                                        '    print("Hello World!")']
                    }
            }
        If the input object is not properly formatted or absent, the default
        code sample will be returned.
        :param - self - arcGIS toolbox metadata object/class
        :returns - rtn_json- a validated code sample json object"""
        rtn_json = {}
        try:
            # if any error due to null or improper formatting,
            # skip to except and return the default code example only
            if self.code_ex == {}:
                raise ValueError("Code example is not available")
            for (title, details) in self.code_ex.items():
                title_str = title
                desc_str_ = details["para"]
                code_str ="\n".join(details["code"])
                if title_str.strip() == "":
                    title_str = "{0}: Code Sample)".format(self.label)
                if desc_str_.strip() == "":
                    details["para"] = "Sample Description: {0}".format(self.summary)
                if not bool(code_str):
                    raise ValueError("Code example is not available")
                rtn_json[title] = {"para": desc_str_, "code": code_str}
            return rtn_json
        except BaseException:
            # generate the default code sample
            title_str = "{0}: Code Sample (1)".format(self.label)
            desc_str_ = '''
                                    <em>    <b>Note</b> : Calling custom toolboxes is only available</em>
                                    <em>    within external python interpreters and script files!</em>
                                    <em>    Code sample will not work in ArcGIS python windows.</em>
                                    <em>    Code sample will not work in ArcGIS python notebooks.</em>'''
            code_str = self.gen_default_code()
            rtn_json = {title_str: {"para": desc_str_, "code": code_str}}
        return rtn_json

# ----------------------------------------------------------------------------


class toolboxMetadata(object):
    def __init__(
        self,
        toolbox_path,
        overwrite=False,
        toolbox_xml_dict={},
        toolbox_xml_attrib_dict={},
        tool_xml_dict={},
        tool_xml_attrib_dict={}):

        source_path = str(Path(toolbox_path).resolve())
        # ------------------------------------------------------------------------
        # set the path property to the toolbox (.pyt file)
        self.toolbox_path = source_path
        # set the toolbox name property
        self.toolbox_name = os.path.splitext(
            os.path.basename(toolbox_path))[0]
        # set the object overwrite property
        self.overwrite = overwrite

        # set the default xml structure dictionaries
        # ------------------------------------------------------------------------
        if not bool(tool_xml_dict):
            # use a default xml structure
            self.tool_xml_dict = DEFAULT_TOOL_XML_STRUCT
        else:
            self.tool_xml_dict = tool_xml_dict
        if not bool(toolbox_xml_dict):
            # use a default xml structure
            self.toolbox_xml_dict = DEFAULT_TOOLBOX_XML_STRUCT
        else:
            self.toolbox_xml_dict = toolbox_xml_dict
        # set the default xml attribute dictionaries
        # ------------------------------------------------------------------------

        self.toolbox_xml_attrib_dict = toolbox_xml_attrib_dict
        if not bool(self.toolbox_xml_attrib_dict):
            self.toolbox_xml_attrib_dict = DEFAULT_XML_ATTRIBS
        self.tool_xml_attrib_dict = tool_xml_attrib_dict
        if not bool(self.tool_xml_attrib_dict):
            self.tool_xml_attrib_dict = DEFAULT_XML_ATTRIBS
        # get general toolbox information
        # ------------------------------------------------------------------------
        # get the name of the current script/module file
        self.mod_name = os.path.splitext(os.path.basename(__file__))[0]

        # import  the Toolbox class object
        self.tb = import_toolbox(self.toolbox_path).Toolbox
        
        # create an instance of the Toolbox
        self.tb_inst = self.tb()

        # get a list of the tools in the Toolbox instances
        self.tb_tools = [t for t in self.tb_inst.tools]
        
        # get the ArcGIS help resources path
        self.arcToolboxHelpPath = os.path.join(
            os.path.abspath(os.path.join(sys.path[0], os.pardir)), "Help\\gp")

        # get toolbox level metadata from toolbox instance attributes
        # ------------------------------------------------------------------------
        # toolbox alias
        self.alias = get_class_attrib(self.tb_inst, "alias", self.toolbox_name)
        # toolbox idPurp/description text
        dflt_idPurp =   '''{0} is an ArcGIS python toolbox.
                                       Contact POC below for more information.'''.format(
                                        self.toolbox_name)

        self.idPurp = get_class_attrib(self.tb_inst, "description", dflt_idPurp)
        # toolbox idAbs/abstract text
        dflt_idAbs = '{0}{1}{2}'.format(
                                self.idPurp,
                                "<br></br>" * 2,
                                self.create_abstract_tool_text(self.tb_tools)
                                )

        self.idAbs = get_class_attrib(self.tb_inst, "idAbs", dflt_idAbs)
        # set derived tool metadata variables
        self.resTitle = self.toolbox_name
        # check for toolbox searchKeys in the toolbox attributes
        self.keyword_master = []
        self.keywords = get_class_attrib(self.tb_inst, "searchKeys", [])

        # generate default keywords from the toolbox
        if not isinstance(self.keywords, list) or not bool(self.keywords):
            self.keywords = set_default_keywords([self.alias, self.toolbox_name])
        self.keyword_master = self.keywords.copy()
        # get the toolbox created and mod date/times
        self.toolbox_dates = get_file_dates(self.toolbox_path)
        self.CreaDate = self.toolbox_dates[1]
        self.CreaTime = self.toolbox_dates[0]
        self.ModDate = self.toolbox_dates[2]
        self.ModTime = self.toolbox_dates[3]
        self.mdDateSt = self.toolbox_dates[6]

        # create default credit info from the ArcGIS portal session
        self.creditsList = create_credits_list()
        dflt_idCredit = "<br></br>".join(self.creditsList)
        self.idCredit = get_class_attrib(self.tb_inst, "idCredit", dflt_idCredit)
        # build a default usage limits statement
        dflt_useLimit =  '''<b>For questions regarding usage limitations, contact:</b>
                                           {0}
                                           {1}
                                           {2}
                                        <br></br>
                                        <b>Disclaimer: **Metadata auto generated with module {3}**</b>
                                        <b>    -For detailed release notes, contact the POC above!</b>
                                        '''.format(
            self.creditsList[0], self.creditsList[1], self.creditsList[2], self.mod_name)
        self.useLimit = get_class_attrib(self.tb_inst, "useLimit", dflt_useLimit)
        # holders for tool metadata objects and xml root objects
        self.def_tool_metas = []
        self.xml_roots = {}
        
        # set default Esri Toolbox metadata values:
        # ------------------------------------------------------------------------
        self.formatName = TOOLBOX_FORMAT_NAME
        self.ArcGISFormat = ARC_GIS_FORMAT
        self.SyncOnce = SYNC_ONCE
        self.minScale = MIN_SCALE
        self.maxScale = MAX_SCALE
        self.ArcGISProfile = ARC_GIS_PROFILE        
        # ------------------------------------------------------------------------

    def generate_toolbox_metadata(self):
        """Method imports creates a toolbox xml metadata object
        given the toolbox metadata structure as an ordered dictionary,
        a dictionary of element attribute values, an instance of the toolbox
        and a toolbox metadata object (self). 
        :param - self - toolbox metadata object
        :returns -tb_meta_dict - dictionary of toolbox metadata path
                                                and root elements
                                 Ex. {xml output path, etree root element}"""               
        if "toolbox" not in self.toolbox_xml_attrib_dict.keys():
            self.toolbox_xml_attrib_dict["toolbox"] = {
                "name": self.toolbox_name,
                "alias": self.alias
                }
        # --------------------------------------------------------------------
        # build the xml metadata structure
        xml_root, xml_elms = build_metadata_structure(
            self.toolbox_xml_dict, self.toolbox_xml_attrib_dict)
        # --------------------------------------------------------------------
        # set the xml element text  with text values from the toolbox class
        xml_elms = set_xml_text_by_class_attrib(self.tb_inst, xml_elms)
        # set the xml element text with text via the toolbox metadata co
        xml_elms = set_xml_text_by_class_attrib(self, xml_elms)
        # --------------------------------------------------------------------
        # create and set the searchKeys sub-element 'keyword' values
        self.keyword_master = set_default_keywords([" ".join(self.keyword_master)])
        for kw in self.keyword_master:
            et_se(xml_elms["searchKeys"], "keyword").text = kw
        tb_meta_dict = {self.toolbox_path + '.xml': xml_root}
        return tb_meta_dict

    # ------------------------------------------------------------------------

    def generate_tool_metadata(self):
        """Method creates a tool xml metadata object
        given the tool metadata structure as an ordered dictionary,
        a dictionary of element attribute values, an instance of the
        tool, an instance of the toolbox, the tool metadata object
        and the toolbox metadata object (self). 
        :param - self - toolbox metadata object
        :returns -xml_roots - dictionary of tool metadata path
                                                and root elements
                                    Ex. {xml output path, etree root element}"""          
        # begin creating the xml data for the tools in the toolbox
        for tb_tool in self.tb_tools:
            def_tool_meta = toolMetadata(self, tb_tool)
            tool_inst = tb_tool()
        
            if "tool" not in self.tool_xml_attrib_dict.keys():
                self.tool_xml_attrib_dict["tool"] = {
                    "xmlns": "",
                    "name": tb_tool.__name__,
                    "displayname": def_tool_meta.label,
                    "toolboxalias": self.alias
                    }
            # --------------------------------------------------------------------
            # build the xml metadata structure
            xml_root, xml_elms = build_metadata_structure(
                self.tool_xml_dict, self.tool_xml_attrib_dict)
            # --------------------------------------------------------------------
            # attribute the  xml element text with values from a tool instance
            xml_elms = set_xml_text_by_class_attrib(tool_inst, xml_elms)
            # attribute the  xml element text with values from a toolbox instance
            xml_elms = set_xml_text_by_class_attrib(self.tb_inst, xml_elms)

            # attribute the remaining  null xml element text items with default values
            # values set in the toolbox/tool code override the defaults
            # set default values from the tool metadata co
            xml_elms = set_xml_text_by_class_attrib(def_tool_meta, xml_elms)
            # set default values from the toolbox metadata co
            xml_elms = set_xml_text_by_class_attrib(self, xml_elms)
            # --------------------------------------------------------------------
            # create and set the searchKeys sub-element 'keyword' values
            for kw in def_tool_meta.keywords:
                et_se(xml_elms["searchKeys"], "keyword").text = kw
            # --------------------------------------------------------------------
            # create and set the code example metadata
            for (title, details) in def_tool_meta.code_ex.items():
                scriptExample = et_se(
                    xml_elms["scriptExamples"], "scriptExample")
                et_se(scriptExample, "title").text = title
                et_se(scriptExample, "para").text = py_text_to_html(
                    details["para"] )
                et_se(scriptExample, "code").text = details["code"]
            # --------------------------------------------------------------------
            # loop the tool's input arguments/params, for each arg:
            # create and set the tool sub-element 'parameters' attributes and values
            if hasattr(tool_inst, "getParameterInfo"):
                t_args = tool_inst.getParameterInfo()
                for a in t_args:
                    # create an new parameter elm and set the xml attributes
                    p_elm = et_se(
                        xml_elms["parameters"],
                        "param", type = a.parameterType, datatype = a.datatype,
                        name = a.name, displayname = a.displayName,
                        direction = a.direction,
                        )
                    # get/set the parameter sub-element 'dialogReference' values
                    et_se(
                        p_elm, "dialogReference").text = py_text_to_html(
                            '<em>' + get_class_attrib(a, "dialogReference", a.displayName
                            ) + '</em><br></br>'
                        )
                    # get p_elm dependencies
                    p_depen = 'N/A'
                    if hasattr(a, 'parameterDependencies'):                    
                        if bool(a.parameterDependencies):
                            p_depen = a.parameterDependencies
                    p_depen = '<u>Dependencies:</u> {0}'.format(p_depen)
                    
                    # get p_elm default
                    p_def = '<u>Default Value:</u> N/A'
                    if hasattr(a, 'valueAsText'):
                        if bool(a.valueAsText):
                            p_def = '<u>Default Value:</u> {0}'.format(a.value)
                        

                    # get p_elm allowed values (up to 10 values)
                    p_filter_type = ''
                    p_filter_list = ''
                    if hasattr(a, 'filter'):
                        if bool(a.filter.type):
                            p_filter_type = a.filter.type
                        if p_filter_type == 'ValueList' and bool(a.filter.list):

                            lead_space = '&#160; ' * 8
                            p_filter_prefix =  '\n<span>{0}-</span>'.format(lead_space)
                            p_filter_list = '<u>Allowed Values:</u>' + \
                                            ((p_filter_prefix).join([''] + a.filter.list[0:10]))
                            if len(a.filter.list) > 10:
                                msg_prefix =  '\n<span>{0}</span>'.format(lead_space)
                                p_filter_list = p_filter_list + (msg_prefix + '<b>*Only first 10 values displayed...*</b>')
                                p_filter_list = p_filter_list + (msg_prefix + '  <b>*See tool parameter for full list...*</b>')
                        if p_filter_type == 'Range' and len(a.filter.list) == 2:
                            p_filter_list = '<u>Allowed Range:</u> Min({0}),  Max ({1})'.format(
                                a.filter.list[0],
                                a.filter.list[1])
                            
                    # create the default pythonReference text
                    py_ref = (
                        '''<u>Python variable name:</u> (<em>{0}</em>)
                           <u>Description:</u> {1} {2} value representing the tool
                           {8}"<em>{3}</em>" {4} parameter.
                           {5}
                           {6}
                            {7}'''.format(
                            a.name, a.parameterType, a.datatype.lower(),
                            a.displayName, a.direction, p_depen, p_def,
                            p_filter_list, (10 * '&#160; '))
                        )

                    # get/set the p_elm sub-element 'pythonReference' attributes
                    et_se(
                        p_elm, "pythonReference").text = py_text_to_html(
                        get_class_attrib(a, "pythonReference", py_ref
                        )
                    )
            # --------------------------------------------------------------------
            # retain metadata and xml variables as attributes for debugging
            self.def_tool_metas.append(def_tool_meta)
            self.xml_roots[def_tool_meta.xml_path] = xml_root
        return self.xml_roots

    # --------------------------------------------------------------------------
    
    def create_abstract_tool_text(self, tb_tools):
        """Method creates the default toolbox metadata abstract
        text statement.  The resulting statement has provides an
        overview and lists out the tools contained in the toolbox. 
        :param - self - toolbox metadata object
        :param - tb_tools - list of toolbox tool classes
        :returns -rtn_text - display text for the toolbox abstract"""         
        tool_lines = ["<b>Included Tools:</b>"]
        for tb_tool in tb_tools:
            tool_inst = tb_tool()
            tool_name = get_class_attrib(tool_inst, "label", tb_tool.__name__)
            tool_cat = get_class_attrib(tool_inst, "category", "None")
            tool_desc = get_class_attrib(tool_inst, "description", "", False, False)
            if not bool(tool_desc):
                tool_desc = get_class_attrib(tool_inst, "usage", "", False, False)
            tool_lines.append(
                "<br></br><b>    - {0}</b> (Category: {1})".format(tool_name, tool_cat)
            )
            for line in tool_desc.splitlines():
                tool_lines.append("<em>        {0}</em>".format(line.strip()))
        rtn_text = "<br></br>".join(tool_lines)
        return rtn_text

    # --------------------------------------------------------------------------
    
    def xml_tree_to_file(self, out_path, xml_root):
        """Method creates or overwrites an xml document
        with the xml etree object provided at the given path. 
        :param - self - toolbox metadata object
        :param - out_path - output path
        :param - xml_root - xml etree root element
        :returns -none - """            
        # skip creating  metadata if the xml exists and overwrite == False
        if (not os.path.exists(out_path)) or self.overwrite:
            t = et_root(xml_root)
            t.write(out_path, encoding="utf-8", xml_declaration=True)
        return
    
    # --------------------------------------------------------------------------
    
    def write_tool_xml_metadata(self):
        """Method writes each tool xml element
        to an output path derived from the 
        generate_tool_metadata method. 
        :param - self - toolbox metadata object
        :returns -none - """             
        xml_roots = self.generate_tool_metadata()
        for out_path, xml_root in xml_roots.items():
            self.xml_tree_to_file(out_path, xml_root)
        return
    
    # --------------------------------------------------------------------------
    
    def write_toolbox_xml_metadata(self):
        """Method writes the toolbox xml element
        to an output path derived from the 
        generate_tool_metadata method. 
        :param - self - toolbox metadata object
        :returns -none - """          
        xml_roots = self.generate_toolbox_metadata()
        for out_path, xml_root in xml_roots.items():
            self.xml_tree_to_file(out_path, xml_root)
        return
    
    # --------------------------------------------------------------------------
    
    def write_all_xml_metadata(self):
        """Method writes the xml metadata for
        the toolbox and all  tools belonging to the
        toolbox to the toolbox (pyt) directory. 
        :param - self - toolbox metadata object
        :returns -none - """           
        self.write_tool_xml_metadata()
        self.write_toolbox_xml_metadata()
        return

# ---------------------------------------------------------------------------

def create_tb_meta(toolbox_path,overwrite=False):
    """Function calls a process that writes
    xml metadata for all toolbox class objects. 
    :param - toolbox_path - path to the toolbox file (.pyt)
    :returns -Boolean - True on success """               
    rtn_val = False
    if not toolbox_path.endswith('.pyt'):
        return
    source_path = str(Path(toolbox_path).resolve())
    try:
        toolboxMetadata(
                                source_path,
                                overwrite
                                ).write_all_xml_metadata()            
        rtn_val =  True
    except:
        warnings.warn('Failed to generate toolbox metadata!')
    return rtn_val

#----------------------------------------------------------------------------

class InvalidFabric(Exception):
    pass

#----------------------------------------------------------------------------


class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the.pyt file)."""
        self.label = __name__
        self.alias = __alias__
        self.description = f'''{__info__}
                                <b><em>For additional toolbox documentation and updates see: <a href="{__orgGitHub__}" target="_blank" STYLE="text-decoration:underline;">NTIA-Performance and Data Analytics on GitHub</a></em></b>
                            '''
        self.CreaDate = __create_date__
        self.ArcGISFormat = __ArcGISFormat__
        self.SyncOnce = __SyncOnce__
        self.ModDate = __version_date__
        self.ArcGISProfile = __ArcGISProfile__
        self.arcToolboxHelpPath = __github_url__
        self.resTitle = self.alias
        self.searchKeys = __searchKeys__
        self.idPurp = self.description
        self.idCredit = __idCreditStr__
        self.useLimit = f'\n\n {__data_restrictions__} \n {__license__}'
        self.formatName = __formatName__
        self.mdDateSt = __version_date__
        
        # List of tool classes associated with this toolbox
        self.tools = [create_fabric_features, create_service_level_dataset]

#----------------------------------------------------------------------------
        
class create_service_level_dataset(object):
    def __init__(self):
        """Define the tool (tool name is BroadbandDataJoin)"""
        self.label = "2. Create Service Level Dataset"
        self.alias = "createServiceLevelDataset"

        self.description = __tools__[0]
        self.usage = f"""{__tools__[0]}
        The input data must be a csv containing the CostQuest fabric
        locations to be analyzed. The outputs can be a spatial dataset
        (Featureclass or shapefile) or a table (GDB table or CSV).
        For spatial outputs, using a GDB featureclass rather than a
        shapefile will have better results as there are size restrictions
        (2 GB) and limits the column name length of shapefiles. The output
        data will contain the selected fabric columns and the resultant
        service level data.

        <b><em>For additional toolbox documentation and updates see: <a href="{__orgGitHub__}" target="_blank" STYLE="text-decoration:underline;">NTIA-Performance and Data Analytics on GitHub</a></em></b>
        
        <b>*Note:</b>
        <em>An internet connection is required as the tool will send</em>
        <em>requests for  data to the <a href="https://broadbandmap.fcc.gov/data-download/nationwide-data?" target="_blank" STYLE="text-decoration:underline;">FCC National Broadband Map</a></em>   

        <b>*Note:</b>
        <em>Only location fabric records with the bsl_flag = True will be included in the output.</em>
            
        {__data_restrictions__}
        
        Service Level Criteria:
        i. How does the BEAD program define an “unserved” location?
        Section I.C.bb. of the NOFO defines unserved locations as locations
        lacking reliable broadband service or with broadband service offering
        speeds below 25 megabits per second (Mbps) downstream/3 Mbps upstream
        at a latency of 100 milliseconds or less. Reliable broadband means
        broadband service that the Broadband DATA Maps show is accessible to
        a location via fiber-optic technology; Cable Modem/ Hybrid fiber-coaxial
        technology; digital subscriber line technology; or terrestrial fixed wireless
        technology utilizing entirely licensed spectrum or using a hybrid of licensed
        and unlicensed spectrum. Locations that are served by satellite or purely
        unlicensed spectrum will also be considered unserved. 

        See: <a href="https://broadbandusa.ntia.gov/sites/default/files/2022-06/BEAD-FAQs.pdf" target="_blank" STYLE="text-decoration:underline;">BEAD FAQ’s</a>                  

        ii. How does the BEAD program define an “underserved” location?
        Section I.C.cc. of the NOFO defines underserved locations as locations
        that are identified as having access to reliable broadband service of
        at least 25 Mbps downstream/3 Mbps upstream but less than 100 Mbps
        downstream/20 Mbps upstream at a latency of 100 milliseconds or less.
        Reliable broadband means broadband service that the Broadband DATA Maps
        show is accessible to a location via fiber-optic technology;
        Cable Modem/Hybrid fiber-coaxial technology; digital subscriber line
        technology; or terrestrial fixed wireless technology utilizing entirely
        licensed spectrum or using a hybrid of licensed and unlicensed spectrum. 
        ed.  

        See: <a href="https://broadbandusa.ntia.gov/sites/default/files/2022-06/BEAD-FAQs.pdf" target="_blank" STYLE="text-decoration:underline;">BEAD FAQ’s</a>


        iii. Applied Service Level Criteria:

        Based on the definition of "Reliable broadband" stated above, NTIA includes
        technology codes listed below in the analysis of a location's max service level.
        BDC codes for "Reliable broadband" deployed technology types:
            •	10 : Copper Wire
            •	40 : Coaxial Cable / HFC
            •	50 : Optical Carrier / Fiber to the Premises
            •	71 : Licensed Terrestrial Fixed Wireless
            •	72 : Licensed-by-Rule Terrestrial Fixed Wireless

        Based on the FCC definition of "low latency" in the BDC data specification,
        NTIA classifies service availability with latency above 100 milliseconds as unserved.
        The BDC dataset indicates low latency status with Boolean codes:
            •	0 : False (Not low latency - above 100 milliseconds)
            •	1 : True (low latency - at or less than 100 milliseconds)
        Resulting Service Levels Defined:
            •	Unserved: Speeds below 25/3 Mbps or NULL OR without low_latency (low_latency=0)
            •	Underserved: Speeds at or above 25/3 Mbps, but Below 100/20 Mbps with low_latency (low_latency=1)
            •	Served: Service Level at or above 100/20 Mbps with low_latency (low_latency=1)
        See: <a href="https://us-fcc.app.box.com/v/bdc-data-downloads-output" target="_blank" STYLE="text-decoration:underline;">FCC's Data Spec. for BDC Public Data Downloads</a>
        """

        self.CreaDate = __create_date__
        self.ArcGISFormat = __ArcGISFormat__
        self.SyncOnce = __SyncOnce__
        self.ModDate = __version_date__
        self.ArcGISProfile = __ArcGISProfile__
        self.arcToolboxHelpPath = __github_url__
        self.resTitle = self.alias
        self.idPurp = self.description
        self.searchKeys = __searchKeys__
        self.idCredit = __idCreditStr__
        self.useLimit = f'\n\n {__data_restrictions__} \n {__license__}'
        self.formatName = __formatName__
        self.mdDateSt = __version_date__
        
        self.default_cols = [
                            'latitude',
                            'longitude',
                            'fcc_rel',
                          ]
    #------------------------------------------------------------------------
    def getParameterInfo(self):
        """Define parameter definitions"""
        params = [
                  arcpy.Parameter(
                      displayName="Location Fabric CSV File",
                      name="location_fabric_csv_file",
                      datatype="DEFile",
                      parameterType="Required",
                      direction="Input"
                  ),                      
                  arcpy.Parameter(
                      displayName="Output Workspace",
                      name="output_workspace",
                      datatype="DEWorkspace",
                      parameterType="Optional",
                      direction="Input"
                  ),
                  arcpy.Parameter(
                      displayName="Output Format",
                      name="output_format",
                      datatype="GPString",
                      parameterType="Required",
                      direction="Input"
                  ),
                  arcpy.Parameter(
                      displayName="Output Columns",
                      name="keep_cols",
                      datatype="GPString",
                      parameterType="Required",
                      direction="Input",
                      multiValue="True"
                  )
                  
                  ]

        # set default values & filters:
        params[1].value = get_default_gdb()
        fnt_lst = ["Feature Class", "Table Only"]
        params[2].filter.type = "ValueList"
        params[2].filter.list = fnt_lst
        params[2].value = fnt_lst[0]

        params[3].filter.list = self.default_cols
        params[3].value = self.default_cols
        
        return params

    #------------------------------------------------------------------------
    
    def isLicensed(self):
        """The tool is licensed"""
        return True

    #------------------------------------------------------------------------
    
    def updateParameters(self, parameters):
        # set the output workspark/gdb    
        if not bool(parameters[1].value):
            parameters[1].value = get_default_gdb
               
        if parameters[0].altered and not parameters[0].hasBeenValidated:
            try:
                csv_cols = list(pd.read_csv(parameters[0].valueAsText,
                                            nrows=1
                                            ).columns)
                parameters[3].filter.list = csv_cols 
                parameters[3].value = self.default_cols
            except:
                pass
                parameters[3].value = self.default_cols
            
        if parameters[3].altered and not parameters[3].hasBeenValidated:
            if parameters[3].value:
                cols = (parameters[3].valueAsText).split(";")
            else:
                cols = self.default_cols
                parameters[3].value = self.default_cols
                parameters[3].setWarningMessage(
                    f'{",".join(self.default_cols)} are required ouput columns!')
                
            if not set(self.default_cols).issubset(set(cols)):
                parameters[3].value = list(set(cols + self.default_cols))
                parameters[3].setWarningMessage(
                    f'{",".join(self.default_cols)} are required ouput columns!')
        return

    #------------------------------------------------------------------------
    
    def updateMessages(self, parameters):
        """Update the tool messages"""
        if parameters[0].altered and not parameters[0].hasBeenValidated:
            if not (parameters[0].valueAsText).endswith('.csv'):
                    parameters[0].setErrorMessage('Unable to read file, must be a CSV')
            else:
                parameters[0].clearMessage()
        else:
            parameters[0].clearMessage()

        return

    #------------------------------------------------------------------------
    
    def execute(self, parameters, messages):
        """Execute the tool"""
        try:

            # gather params
            loc_fab_path = parameters[0].valueAsText
            wksp = parameters[1].valueAsText
            out_fmt = parameters[2].valueAsText
            include_cols = (parameters[3].valueAsText).split(";")

            # default NTIA tech codes
            tech_codes = [10,40,50,71,72]
            
            # get the info required to query the FCC BDC data (state and fabric version)
            fabric_vers = get_state_fabric_versions(loc_fab_path)

            # collelct the metadata needed to request the matching bdc data
            zipped_csv_files = []
            for i in fabric_vers:
                fabric_date_str, bdc_update_date_str, zips = get_fcc_bdc_data(i['fcc_rel'],
                                                                  i['state_fips'],
                                                                  tech_codes)
                if bool(zips):
                    zipped_csv_files.extend(zips)

            # set the output layer name and alias
            output_alias = f'Service Levels: BDC update({bdc_update_date_str}), Location Fabric({fabric_date_str})'
            output_name_fmt = f'Service Levels Fabic{fabric_date_str}_BDC{bdc_update_date_str}'
            
            # set the output layer name
            out_name = get_default_output_name([output_name_fmt],
                                                wksp)
                        
            # read the location fabric data and the BDC data into dataframes 
            bdc_data_df = read_bdc_data(zipped_csv_files, tech_codes)

            if bdc_data_df is None: 
                raise InvalidFabric(fabric_version)

            # read the fabric location id's
            location_fabric_df = read_location_fabric(loc_fab_path)

            # generate the service level tiers based on the fabric and current BDC data
            bsl_max_srvc_lvls_df = get_bsl_max_service_levels(location_fabric_df, bdc_data_df)

            # set the output columns
            fabric_out_cols = list(set(include_cols + self.default_cols))
            srvc_out_cols =[c for c in bsl_max_srvc_lvls_df.columns.tolist()
                            if c not in list(set(location_fabric_df.columns.tolist()
                                                 + bdc_data_df.columns.tolist()))]
            
            # if non default columns are selected, re-read the fabric
            if sorted(include_cols) != sorted(self.default_cols):
                include_cols = list(set(fabric_out_cols + ['location_id']))
                location_fabric_df = read_location_fabric(loc_fab_path,
                                                  columns=include_cols)
            
            # join the farbic to the service level data
            bsl_max_srvc_lvls_df = location_fabric_df.merge(bsl_max_srvc_lvls_df,  
                                                    on='location_id',
                                                    how='left',
                                                    suffixes=('', '_df2_'))

            # Remove un-needed or duplicated columns
            dup_cols = [c for c in bsl_max_srvc_lvls_df.columns
                            if c.endswith('_df2_')
                            or (not c in fabric_out_cols
                            and not c in srvc_out_cols)]
            
            bsl_max_srvc_lvls_df = bsl_max_srvc_lvls_df.drop(columns=dup_cols)

           
            # clear up memory
            clean_df_memory([location_fabric_df])

            arcpy.AddMessage(f'Saving results...')
            # set the output name
            out_name = get_iterative_filename(out_name, wksp)
            # determine the workspace type and which file extensions are needed
            wksp_type = arcpy.Describe(wksp).workspaceType
            if wksp_type == 'FileSystem':
                if out_fmt == "Feature Class":
                    if not out_name.lower().endswith('.shp'):
                        out_name = f'{out_name}.shp'
                else:
                    if not out_name.lower().endswith('.csv'):
                        out_name = f'{out_name}.csv'                    
            # set the output path            
            out_path = os.path.join(wksp, out_name)

            
            if out_fmt == 'Feature Class':
                # Convert a dataframe (bsl_max_srvc_lvls_df) to a spatial dataframe
                sdf = pd.DataFrame.spatial.from_xy(df=bsl_max_srvc_lvls_df,
                                                    x_column='longitude',
                                                    y_column='latitude',
                                                    sr=4326)
                # Save to feature class 
                sdf.spatial.to_featureclass(location=out_path,
                                            overwrite=True)
            else:
                if wksp_type == 'FileSystem':
                    bsl_max_srvc_lvls_df.to_csv(
                                                out_path,
                                                index=False)
                else:
                    bsl_max_srvc_lvls_df.spatial.to_table(
                                        location=out_path,
                                        overwrite=True)

            # clear up memory
            clean_df_memory([bsl_max_srvc_lvls_df])
            
            # set the item alias for gdb items
            if wksp_type != 'FileSystem':
                arcpy.AlterAliasName(out_path, output_alias)

            arcpy.AddMessage(f'Results saved to: {out_path}')

        except InvalidFabric:
            arcpy.AddError('''Check the input location fabric file and internet connection\n 
            NO FCC BDC reporting data was available for processing''')
       
        except:
            # Catch any other default errors 
            e = sys.exc_info()[1]
            arcpy.AddError(e)
          
    #------------------------------------------------------------------------

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return
    
    #------------------------------------------------------------------------


#----------------------------------------------------------------------------
        
class create_fabric_features(object):
    def __init__(self):
        """Define the tool (tool name is BroadbandDataJoin)"""
        self.label = "1. Create Location Fabric BSL Features"
        self.alias = "createLocationFabricBslFeatures"

        self.description = __tools__[1]
        self.usage = f"""{__tools__[1]}

        <b><em>For additional toolbox documentation and updates see: <a href="{__orgGitHub__}" target="_blank" STYLE="text-decoration:underline;">NTIA-Performance and Data Analytics on GitHub</a></em></b>
        
        <b>*Note:</b>
            <em>Only location fabric records with the bsl_flag = True will be included in the output.</em>
        
        {__data_restrictions__}
        """

        self.CreaDate = __create_date__
        self.ArcGISFormat = __ArcGISFormat__
        self.SyncOnce = __SyncOnce__
        self.ModDate = __version_date__
        self.ArcGISProfile = __ArcGISProfile__
        self.arcToolboxHelpPath = __github_url__
        self.resTitle = self.alias
        self.idPurp = self.description
        self.searchKeys = __searchKeys__
        self.idCredit = __idCreditStr__
        self.useLimit = f'\n\n {__data_restrictions__} \n {__license__}'
        self.formatName = __formatName__
        self.mdDateSt = __version_date__
        
        self.default_cols = [
                            'fcc_rel',
                            ]
    #------------------------------------------------------------------------
    def getParameterInfo(self):
        """Define parameter definitions"""
        params = [
                  arcpy.Parameter(
                      displayName="Location Fabric CSV File",
                      name="location_fabric_csv_file",
                      datatype="DEFile",
                      parameterType="Required",
                      direction="Input"
                  ),                      
                  arcpy.Parameter(
                      displayName="Output Workspace",
                      name="output_workspace",
                      datatype="DEWorkspace",
                      parameterType="Optional",
                      direction="Input"
                  ),
                  arcpy.Parameter(
                      displayName="Output Name",
                      name="output_name",
                      datatype="GPString",
                      parameterType="Optional",
                      direction="Input"
                  ),
                  arcpy.Parameter(
                      displayName="Output Columns",
                      name="keep_cols",
                      datatype="GPString",
                      parameterType="Required",
                      direction="Input",
                      multiValue="True"
                  )
                  
                  ]

        # set default values & filters:
        params[1].value = get_default_gdb()
        params[3].filter.list = self.default_cols
        params[3].value = self.default_cols
        
        return params

    #------------------------------------------------------------------------
    
    def isLicensed(self):
        """The tool is licensed"""
        return True

    #------------------------------------------------------------------------
    
    def updateParameters(self, parameters):
        # set the output workspark/gdb    
        if not bool(parameters[1].value):
            parameters[1].value = get_default_gdb
               
        if parameters[0].altered and not parameters[0].hasBeenValidated:
            try:
                csv_cols = list(pd.read_csv(parameters[0].valueAsText,
                                            nrows=1
                                            ).columns)
                parameters[3].filter.list = csv_cols 
                parameters[3].value = self.default_cols
            except:
                pass
                parameters[3].value = self.default_cols
                
        if parameters[3].altered and not parameters[3].hasBeenValidated:
            if parameters[3].value:
                cols = (parameters[3].valueAsText).split(";")
            else:
                cols = self.default_cols
                parameters[3].value = self.default_cols
                parameters[3].setWarningMessage(
                    f'{",".join(self.default_cols)} are required ouput columns!')
            if not set(self.default_cols).issubset(set(cols)):
                parameters[3].value = list(set(cols + self.default_cols))
                parameters[3].setWarningMessage(
                    f'{",".join(self.default_cols)} are required ouput columns!')

        return

    #------------------------------------------------------------------------
    
    def updateMessages(self, parameters):
        """Update the tool messages"""
        if parameters[0].altered and not parameters[0].hasBeenValidated:
            if not (parameters[0].valueAsText).endswith('.csv'):
                    parameters[0].setErrorMessage('Unable to read file, must be a CSV')
            else:
                parameters[0].clearMessage()
        else:
            parameters[0].clearMessage()

        return

    #------------------------------------------------------------------------
    
    def execute(self, parameters, messages):
        """Execute the tool"""
        try:

            # gather params
            loc_fab_path = parameters[0].valueAsText
            wksp = parameters[1].valueAsText
            out_name = parameters[2].valueAsText
            include_cols = (parameters[3].valueAsText).split(";")

            # set the output file name
            out_name, ext = os.path.splitext(
                                os.path.basename(
                                    parameters[0].valueAsText))

            # make sure the output name is valid
            out_name = get_default_output_name([out_name],
                                                wksp)

            # determine the workspace type and which file extensions are needed
            wksp_type = arcpy.Describe(wksp).workspaceType
            if wksp_type == 'FileSystem':
                if not out_name.lower().endswith('.shp'):
                    out_name = f'{out_name}.shp'
       
            # set the output path            
            out_path = os.path.join(wksp, out_name)
            
            # set the output alias
            output_alias = f'Location Fabric'

            req_cols = ['latitude','longitude']

            read_cols = list(set(req_cols + include_cols))
            location_fabric_df = read_location_fabric(loc_fab_path=loc_fab_path,
                                                      columns=read_cols,
                                                      bsl_flag=True)
                        
            # Convert a dataframe (bsl_max_srvc_lvls_df) to a spatial dataframe
            sdf = pd.DataFrame.spatial.from_xy(df=location_fabric_df,
                                               x_column='longitude',
                                               y_column='latitude',
                                               sr=4326)

            # Ensure the geometry column is included
            sdf = sdf[list(set(include_cols + ['SHAPE']))]
            
            # Save to feature class 
            sdf.spatial.to_featureclass(location=out_path,
                                        overwrite=True)

            # clear up memory
            clean_df_memory([location_fabric_df])
            
            # set the item alias for gdb items
            if wksp_type != 'FileSystem':
                arcpy.AlterAliasName(out_path, output_alias)

            arcpy.AddMessage(f'Results saved to: {out_path}')

        except InvalidFabric:
            arcpy.AddError('''Fields to generate location fabric features''')
       
        except:
            # Catch any other default errors 
            e = sys.exc_info()[1]
            arcpy.AddError(e)
          
    #------------------------------------------------------------------------

    def postExecute(self, parameters):
        """This method takes place after outputs are processed and
        added to the display."""
        return
    
    #------------------------------------------------------------------------
    
# generate toolbox and tool xml metadata files
tb_meta = create_tb_meta(__file__, True)
