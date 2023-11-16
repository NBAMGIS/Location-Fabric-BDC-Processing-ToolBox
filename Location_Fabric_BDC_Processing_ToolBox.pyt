# -*- coding: utf-8 -*-
#------------------------------------------------------------------------------
# metadata
__name__            = 'Location_Fabric_BDC_Processing_ToolBox.pyt'
__alias__           = 'Location Fabric And BDC Processing ToolBox'
__author__          = 'ahamptonTIA'
__credits__         = ['ahamptonTIA']
__version__         = '0.0.2'
__maintainer__      = 'ahamptonTIA'
__email__           = 'https://github.com/ahamptonTIA'
__status__          = 'Alpha'
__create_date__     = '20231011'  
__version_date__    = '20231114'
__info__ = \
    '''
    Description:
        The Location Fabric BDC Processing ToolBox is a collection of 
        ArcGIS python tools which make processing large volume 
        Location Fabric and FCC BDC data possible within ArcGIS Pro. 
    '''
#----------------------------------------------------------------------------

import os, sys, gc, requests, datetime, json, tempfile, arcpy
import pandas as pd
import arcgis
from arcgis.features import GeoAccessor, GeoSeriesAccessor

#----------------------------------------------------------------------------

def clean_df_memory(df):
    """Clears and releases memory used by pandas dataframes.
    Parameters:
    -----------
    df: pandas.DataFrame or a list of dataframes, required
        DataFrames to be removed from memory
    Returns
    -------
        None
    """    
    del(df)
    # Manually trigger the garbage collector to release memory
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
            Path to the default default folder.
    """    
    try:
        aprx = arcpy.mp.ArcGISProject('CURRENT')
        _dir = aprx.homeFolder
    except:
        _dir = tempfile.gettempdir()
    return _dir

#----------------------------------------------------------------------------

def get_defualt_output_name(parameters=[], workspace=None):
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
        A string containing the validated defualt feature or table
        name based on two input file names.
    """

    s = '_'.join(parameters)

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
                d['fcc_rel'] = datetime.datetime.strptime(
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
            The max off set in days.  If no dates
            fall within this range, none will be returned
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
            arcpy.AddWarning('No dates found within specified range!') 
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
            Defualt, 128
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
    except:
        arcpy.AddError(f'Failed to download zip: {url}')

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
        # set the defualt headers for the FCC Broadband Map Download homepage
        headers =   {
                    "accept": "text/html",
                    "User-Agent": "ArcGIS Python- ",
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
        filing_dict = {datetime.datetime.strptime(l['filing_subtype'], "%B %d, %Y"):
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

                                   
        bdc_date = datetime.datetime.strptime(response.json()['data'][0]['last_updated_date'],
                                              "%Y-%m-%dT%H:%M:%S.%fZ"
                                              )

        f_date_str = fabric_version.strftime("%Y%m%d")
        bdc_date_str = bdc_date.strftime("%Y%m%d")
        
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
        itr_cnt = 1
        for i in filtered_file_set:
            url = f'{dl_url_base}/{i["id"]}/{file_numb_id}'
            arcpy.AddMessage(f'Downloading: ({itr_cnt}) of {(len(filtered_file_set))} for state fips:{state}')
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
            itr_cnt += 1
        arcpy.AddMessage(f'Total files downloaded: {len(zips)} for state fips:{state}')

        
        return (f_date_str, bdc_date_str, zips)

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
        bdc_df : pandas.DataFrame
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
            bdc_df = pd.concat(dataframes)
        elif len(dataframes) == 1:
            bdc_df = dataframes[0].copy(deep=True)
    
        # Clear initial file read dataframes to conserve resources/memeory
        clean_df_memory(dataframes)

        # Print a count of BDC records 
        arcpy.AddMessage(f"Relevant BDC record count:  {len(bdc_df):,}")
        return bdc_df
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
        loc_fab_df : pandas.DataFrame
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
    loc_fab_df = pd.read_csv(
                             loc_fab_path, 
                             dtype=str,
                             usecols=read_columns
                             )

    # Filter to ensure only BSL's are included in the analysis
    if bsl_flag: 
        loc_fab_df = loc_fab_df[loc_fab_df['bsl_flag']=='True']   

    loc_fab_df = loc_fab_df[columns]
    # Print a count  of BSL records
    arcpy.AddMessage(f"Total BSL's:  {len(loc_fab_df):,}")
    return loc_fab_df

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
def get_bsl_max_service_levels(loc_fab_df, bdc_df):
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
    df = loc_fab_df.merge(bdc_df, on='location_id', how='left', suffixes=('', '_df2_'))

    # Remove columns duplicated in the join
    dup_cols = [c for c in df.columns if c.endswith('_df2_')]
    df = df.drop(columns=dup_cols)

    # Clear up some memmory by removing the BDC and location fabric dataframes
    clean_df_memory([bdc_df])

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
            
class InvalidFabric(Exception):
    pass
#----------------------------------------------------------------------------
    
class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Location_Fabric_BDC_Toolbox"
        self.alias = "Location Fabric BDC Toolbox"

        # List of tool classes associated with this toolbox
        self.tools = [create_service_level_dataset]

#----------------------------------------------------------------------------
        
class create_service_level_dataset(object):
    def __init__(self):
        """Define the tool (tool name is BroadbandDataJoin)"""
        self.toolname = "CreateServiceLevelDataset"

        self.label = "Create Service Level Dataset"
        self.description = """Creates a point layer or table representing the highest
                           reported service levels defined by NTIA BEAD program as
                           reliable technologies which include Copper Wire,Coaxial
                           Cable/HFC, Optical Carrier/Fiber to the Premises,
                           Licensed Terrestrial Fixed Wireless and , Licensed-by-Rule
                           Terrestrial Fixed Wireless."""

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
                f_date_str, bdc_date_str, zips = get_fcc_bdc_data(i['fcc_rel'],
                                                                  i['state_fips'],
                                                                  tech_codes)
                if bool(zips):
                    zipped_csv_files.extend(zips)

            # set the output layer name and alias
            out_alias = f'Service Levels: BDC update({bdc_date_str}), Location Fabric({f_date_str})'
            out_name_fmt = f'Service Levels Fabic{f_date_str}_BDC{bdc_date_str}'
            
            # set the output layer name
            out_name = get_defualt_output_name([out_name_fmt],
                                                wksp)
                        
            # read the location fabric data and the BDC data into dataframes 
            bdc_df = read_bdc_data(zipped_csv_files, tech_codes)

            if bdc_df is None: 
                raise InvalidFabric(fabric_version)

            # read the fabric location id's
            loc_fab_df = read_location_fabric(loc_fab_path)

            # generate the service level tiers based on the fabric and current BDC data
            bsl_max_srvc_lvls_df = get_bsl_max_service_levels(loc_fab_df, bdc_df)

            # set the output columns
            fabric_out_cols = list(set(include_cols + self.default_cols))
            srvc_out_cols =[c for c in bsl_max_srvc_lvls_df.columns.tolist()
                            if c not in list(set(loc_fab_df.columns.tolist()
                                                 + bdc_df.columns.tolist()))]
            
            # if non defualt columns are selected, re-read the fabric
            if sorted(include_cols) != sorted(self.default_cols):
                include_cols = list(set(fabric_out_cols + ['location_id']))
                loc_fab_df = read_location_fabric(loc_fab_path,
                                                  columns=include_cols)
            
            # join the farbic to the service level data
            bsl_max_srvc_lvls_df = loc_fab_df.merge(bsl_max_srvc_lvls_df,  
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
            clean_df_memory([loc_fab_df])

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
                arcpy.AlterAliasName(out_path, out_alias)

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
