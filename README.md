<img src="https://www.ntia.gov/themes/custom/ntia_uswds//img/NTIAlogo-official.svg" alt="NTIA Logo" width="50em" align="center">

## BEAD: Location Fabric BDC Processing ToolBox

#### ArcGIS Python Toolbox for processing Location Fabric and FCC BDC data.

---

## Purpose:

##### The Location Fabric BDC Processing Toolbox is a collection of ArcGIS python tools which make processing large volume Location Fabric and FCC BDC data more streamlined and capable within ArcGIS Pro.

---

## Quick Start:

   - See: [Add an existing toolbox to a project](https://pro.arcgis.com/en/pro-app/latest/help/projects/connect-to-a-toolbox.htm#:~:text=use%20its%20tools.-,Add%20an%20existing%20toolbox%20to%20a%20project,-You%20can%20add)

---

## Included Tools:

### 1. Create Location Fabric BSL Features:
#### Tool Overview:      
   - *Creates a point layer from a location fabric dataset using user specified fields for output.  The input data must be a CSV file containing the CostQuest fabric locations to be analyzed.*

      - **Notes:**
   
         - **Only location fabric records with the bsl_flag = True will be included in the output.**
        
      - **Data Restrictions:**
         - *Prior to sharing results, please verify the output dataset fields/columns meet the distribution requirements in accordance with your organization’s signed license agreement with CostQuest Associates.*
         - *See: [Why Do I Need a Fabric License?](https://help.bdc.fcc.gov/hc/en-us/articles/10419121200923-How-Entities-Can-Access-the-Location-Fabric-)*
#### Tool Code Sample:  
```python
# import the toolbox as a module
import arcpy
arcpy.ImportToolbox(r'C:\Projects\ArcGISPro_projects\BCD_Location_Fabric_Layer_Tool\Location_Fabric_BDC_Processing_ToolBox.pyt',
                    r'Location Fabric And BDC Processing ToolBox')

# call the tool and return the output
result = arcpy.arcpy.create_fabric_features_LocationFabricAndBDCProcessingToolBox(
                                   location_fabric_csv_file,    #Location Fabric CSV File- Type(File)
                                   output_workspace,            #Output Workspace- Type(Workspace)
                                   output_name,                 #Output Name- Type(String)
                                   keep_cols                   #Output Columns- Type(String)
                                   )
```
---

### 4. Create Service Level Dataset:
#### Tool Overview:        
   - *Creates a point layer or table representing the highest reported service levels defined by NTIA BEAD program as reliable technologies which include Copper Wire, Coaxial Cable/HFC, Optical Carrier/Fiber to the Premises, Licensed Terrestrial Fixed Wireless and, Licensed-by-Rule Terrestrial Fixed Wireless.
    The input data **must be a CSV file** containing the CostQuest fabric locations to be analyzed. The outputs can be a spatial dataset (Featureclass or shapefile) or a table (GDB table or CSV).
    For spatial outputs, using a GDB featureclass rather than a shapefile will have better results as there are size restrictions (2 GB) and limits the column name length of shapefiles. The output data will contain the selected fabric columns and the resultant service level data.*
    
      - **Notes:**
         - **An internet connection is required as the tool will send requests for  data to the [FCC National Broadband Map](https://broadbandmap.fcc.gov/data-download/nationwide-data?)**
         - **Only location fabric records with the bsl_flag = True will be included in the output.**
        
      - **Data Restrictions:**
         - *Prior to sharing results, please verify the output dataset fields/columns meet the distribution requirements in accordance with your organization’s signed license agreement with CostQuest Associates.*
         - *See: [Why Do I Need a Fabric License?](https://help.bdc.fcc.gov/hc/en-us/articles/10419121200923-How-Entities-Can-Access-the-Location-Fabric-)*         

#### Tool Methodology:
   - **Service Level Criteria:**
     
      - **How does the BEAD program define an “unserved” location?**
   
         - *Section I.C.bb. of the NOFO defines unserved locations as locations lacking reliable broadband service or with broadband service offering speeds below 25 megabits per second (Mbps) downstream/3 Mbps upstream at a latency of 100 milliseconds or less. Reliable broadband means broadband service that the Broadband DATA Maps show is accessible to a location via fiber-optic technology; Cable Modem/ Hybrid fiber-coaxial technology; digital subscriber line technology; or terrestrial fixed wireless technology utilizing entirely licensed spectrum or using a hybrid of licensed and unlicensed spectrum. Locations that are served by satellite or purely unlicensed spectrum will also be considered unserved.*
         - *See: [BEAD FAQ’s](https://broadbandusa.ntia.gov/sites/default/files/2022-06/BEAD-FAQs.pdf)*             

      - **How does the BEAD program define an “underserved” location?**
    Section I.C.cc. of the NOFO defines underserved locations as locations that are identified as having access to reliable broadband service ofat least 25 Mbps downstream/3 Mbps upstream but less than 100 Mbps downstream/20 Mbps upstream at a latency of 100 milliseconds or less. Reliable broadband means broadband service that the Broadband DATA Maps show is accessible to a location via fiber-optic technology; Cable Modem/Hybrid fiber-coaxial technology; digital subscriber line technology; or terrestrial fixed wireless technology utilizing entirely licensed spectrum or using a hybrid of licensed and unlicensed spectrum.* 

         - *See: [BEAD FAQ’s](https://broadbandusa.ntia.gov/sites/default/files/2022-06/BEAD-FAQs.pdf)*      

   - **Applied Service Level Criteria:**
      - Based on the definition of "Reliable broadband" stated above, NTIA includes technology codes listed below in the analysis of a location's max service level. BDC codes for "Reliable broadband" deployed technology types:
        
         - 10 : Copper Wire
         - 40 : Coaxial Cable / HFC
         - 50 : Optical Carrier / Fiber to the Premises
         - 71 : Licensed Terrestrial Fixed Wireless
         - 72 : Licensed-by-Rule Terrestrial Fixed Wireless

      - Based on the FCC definition of "low latency" in the BDC data specification, NTIA classifies service availability with latency above 100 milliseconds as unserved. The BDC dataset indicates low latency status with Boolean codes:
         - 0 : False (Not low latency - above 100 milliseconds)
         - 1 : True (low latency - at or less than 100 milliseconds)
      - Resulting Service Levels Defined:
         - Unserved: Speeds below 25/3 Mbps or NULL OR without low_latency (low_latency=0)
         - Underserved: Speeds at or above 25/3 Mbps, but Below 100/20 Mbps with low_latency (low_latency=1)
         - Served: Service Level at or above 100/20 Mbps with low_latency (low_latency=1)
         - 
   - See: [FCC's Data Spec. for BDC Public Data Downloads](https://us-fcc.app.box.com/v/bdc-data-downloads-output)

#### Tool Code Sample:  
```python
# import the toolbox as a module
import arcpy
arcpy.ImportToolbox(r'C:\Projects\ArcGISPro_projects\BCD_Location_Fabric_Layer_Tool\Location_Fabric_BDC_Processing_ToolBox.pyt',
                    r'LocationFabricAndBDCProcessingToolBox')

# call the tool and return the output
result = arcpy.create_service_level_dataset_LocationFabricAndBDCProcessingToolBox(
                    location_fabric_csv_file    #Location Fabric CSV File- Type(File),
                    output_workspace            #Output Workspace- Type(Workspace),
                    output_format               #Output Format- Type(String),
                    keep_cols                   #Output Columns- Type(String)
                    )
```
    
---

## Environment/Dependencies:

This toolbox was developed and tested with ArcGIS Pro v3.0.0. 

### ArcGIS version:
   - Tested and developed with 3.0+
     
### Python version:
   - Tested and developed with 3.7+
     
#### Python dependancies:
   - pandas

---

## Contributing:

This project is open source and contributions are welcome in the form of comments. 
If you find any errors or have suggestions for improvement, please open an issue on the GitHub repository.

---
## Additional Credits:
   - [**pyt_meta (source code):**](https://github.com/GeoCodable/pyt_meta)
      - Python toolbox metadata add-on or pyt_meta is a module that contains classes and functions that enable automated xml metadata file generation for ArcGIS toolboxes and/or any tools contained in a given toolbox. The pyt_meta module enables default metadata value generation based on the toolbox and tool class attributes/properties and the ArcGIS portal user profile. When geospatial developers wish to override and explicitly control metadata or values, the module enables developers to have full control and access to the xml metadata keys directly within the python toolbox code. As an added benefit among toolbox/tool deployments, the maintenance and packaging of XML support file documents can be eliminated from the process. This results in less file dependencies and increased toolbox deployment reliability and efficient.
      - For more info see: [pyt_meta](https://github.com/GeoCodable/pyt_meta)
        
## GitHub repository License:

See [LICENSE](./LICENSE.md).

---

## Contact:

For technical questions about or comments about this sample notebook, contact [NTIA-Performance & Data Analytics](mailto:nbam@ntia.gov)


U.S. Department of Commerce | National Telecommunications and Information Administration (NTIA) | Office of Internet Connectivity and Growth (OICG) | Performance & Data Analytics 

---
