import requests
import pandas as pd
import xml.etree.ElementTree as ET
import pyodbc
import csv
from requests.auth import HTTPBasicAuth

# Load OIDs from CSV file
oids = []
with open("oids.csv", newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        oids.append(row["OID"].strip())

# SQL Server connection
conn = pyodbc.connect(
    Driver="SQL Server",
    Server="mavenddwva002",
    Database="REFERENCE",
    Trusted_Connection="Yes"
)
cursor = conn.cursor()

# Drop and recreate the table with NVARCHAR(MAX) where appropriate
cursor.execute("""
IF OBJECT_ID('REFERENCE.dbo.ValueSetConcepts_VSAC', 'U') IS NOT NULL
    DROP TABLE REFERENCE.dbo.ValueSetConcepts_VSAC;

CREATE TABLE REFERENCE.dbo.ValueSetConcepts_VSAC (
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
conn.commit()

# Function to extract concept data from VSAC XML
def extract_concepts(xml_content):
    ns = {'ns0': 'urn:ihe:iti:svs:2008'}
    root = ET.fromstring(xml_content)

    value_set = root.find('.//ns0:DescribedValueSet', ns)
    valueset_oid = value_set.get('ID') if value_set is not None else ''
    valueset_name = value_set.get('displayName', '') if value_set is not None else ''

    concepts = []
    for concept in root.findall('.//ns0:Concept', ns):
        concept_data = {
            "Code": concept.get("code", ""),
            "DisplayName": concept.get("displayName", ""),
            "ValueSetOID": valueset_oid,
            "ValuesetName": valueset_name,
            "CodeSystem": concept.get("codeSystem", ""),
            "CodeSystemName": concept.get("codeSystemName", ""),
            "CodeSystemVersion": concept.get("codeSystemVersion", "")
        }
        concepts.append(concept_data)

    return concepts

# VSAC API setup
vsac_url = "https://vsac.nlm.nih.gov/vsac/svs/RetrieveMultipleValueSets"
api_key = "d9c7efa3-7158-4853-8d14-4ae26a3d9a8e"  # Replace with your UMLS API key
auth = HTTPBasicAuth("apikey", api_key)

all_concepts = []

# Request headers to avoid compression issues
headers = {
    "Accept-Encoding": "identity"
}

# Download and parse each OID
for oid in oids:
    print(f"Processing OID: {oid}")
    try:
        response = requests.get(vsac_url, params={"id": oid}, auth=auth, headers=headers, timeout=60)

        if response.status_code == 200:
            try:
                concepts = extract_concepts(response.text)
                all_concepts.extend(concepts)
            except ET.ParseError as e:
                print(f"XML parse error for OID {oid}: {e}")
                with open(f"error_response_{oid.replace(':', '_')}.xml", "w", encoding="utf-8") as f:
                    f.write(response.text)
                continue
        else:
            print(f"Failed to retrieve OID {oid}: Status {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed for OID {oid}: {e}")
        continue

# Convert to DataFrame and enforce field order
ordered_cols = [
    "Code",
    "DisplayName",
    "ValueSetOID",
    "ValuesetName",
    "CodeSystem",
    "CodeSystemName",
    "CodeSystemVersion"
]

df = pd.DataFrame(all_concepts, columns=ordered_cols)

# Clean whitespace
for col in df.columns:
    df[col] = df[col].astype(str).str.strip()

# Upsert using SQL MERGE
upsert_sql = """
MERGE REFERENCE.dbo.ValueSetConcepts_VSAC AS target
USING (SELECT ? AS Code, ? AS DisplayName, ? AS ValueSetOID, ? AS ValuesetName, ? AS CodeSystem, ? AS CodeSystemName, ? AS CodeSystemVersion) AS source
ON target.ValueSetOID = source.ValueSetOID AND target.Code = source.Code
WHEN MATCHED THEN 
    UPDATE SET 
        DisplayName = source.DisplayName,
        ValuesetName = source.ValuesetName,
        CodeSystem = source.CodeSystem,
        CodeSystemName = source.CodeSystemName,
        CodeSystemVersion = source.CodeSystemVersion
WHEN NOT MATCHED THEN
    INSERT (Code, DisplayName, ValueSetOID, ValuesetName, CodeSystem, CodeSystemName, CodeSystemVersion)
    VALUES (source.Code, source.DisplayName, source.ValueSetOID, source.ValuesetName, source.CodeSystem, source.CodeSystemName, source.CodeSystemVersion);
"""

# Prepare and insert
records = [tuple(row[col] for col in ordered_cols) for _, row in df.iterrows()]

for record in records:
    cursor.execute(upsert_sql, record)

conn.commit()
cursor.close()
conn.close()

print(f"Upserted {len(records)} concept records.")
