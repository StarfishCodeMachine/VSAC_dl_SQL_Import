# Name: VSAC_dl_SQL_Import
# Author: Wesley McNeely
# Description: A Python 3.8 script that uses the VSAC API to download Value Sets and uploads them to a SQL Server database.
# Files: VSAC_dl_SQL_Import.py, oid.csv
# Instructions:

1) change     Server="SERVERNAME"
                Database="DBNAME"
2) change from     Trusted_Connection="Yes" to user and password if needed
3) if your table already exists and you are updating your table, comment out

       cursor.execute("""
        IF OBJECT_ID('DBNAME.dbo.ValueSetConcepts_VSAC', 'U') IS NOT NULL
            DROP TABLE DBNAME.dbo.ValueSetConcepts_VSAC;
        CREATE TABLE DBNAME.dbo.ValueSetConcepts_VSAC (
            Code NVARCHAR(255),
            DisplayName NVARCHAR(MAX),
            ValueSetOID NVARCHAR(255),
            ValuesetName NVARCHAR(MAX),
            CodeSystem NVARCHAR(255),
            CodeSystemName NVARCHAR(MAX),
            CodeSystemVersion NVARCHAR(MAX),
            PRIMARY KEY (ValueSetOID, Code)
        )
        """)
   
5)  api_key = "APIKEY"  # Replace with your UMLS API key
6)  update the file oid.csv to include the oids you want to download. the first column is just for reference.
