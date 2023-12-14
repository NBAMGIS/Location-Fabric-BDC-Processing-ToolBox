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
    
   - Creates a point layer from a location fabric dataset using user specified fields for output.

   - **Note:**
   
      - **Only location fabric records with the bsl_flag = True will be included in the output.**
        

### 4. Create Service Level Dataset:
        
   - Creates a point layer or table representing the highest reported service levels defined by NTIA BEAD program as reliable technologies which include Copper Wire, Coaxial Cable/HFC, Optical Carrier/Fiber to the Premises, Licensed Terrestrial Fixed Wireless and, Licensed-by-Rule Terrestrial Fixed Wireless.
    The input data **must be a CSV file** containing the CostQuest fabric locations to be analyzed. The outputs can be a spatial dataset (Featureclass or shapefile) or a table (GDB table or CSV).
    For spatial outputs, using a GDB featureclass rather than a shapefile will have better results as there are size restrictions (2 GB) and limits the column name length of shapefiles. The output data will contain the selected fabric columns and the resultant service level data.
    
   - **Note:**
      - **An internet connection is required as the tool will send requests for  data to the [FCC National Broadband Map](https://broadbandmap.fcc.gov/data-download/nationwide-data?)**
      
   - **Note:**
      - **Only location fabric records with the bsl_flag = True will be included in the output.**

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

## GitHub repository License:

See [LICENSE](./LICENSE.md).

---

## Contact:

For technical questions about or comments about this sample notebook, contact [NTIA-Performance & Data Analytics](mailto:nbam@ntia.gov)


U.S. Department of Commerce | National Telecommunications and Information Administration (NTIA) | Office of Internet Connectivity and Growth (OICG) | Performance & Data Analytics 

---
