import jaydebeapi
import os
import subprocess
import datetime
import numpy as np
import pandas as pd
import csv
import datetime
import gspread
from datetime import timedelta
from datetime import date
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from oauth2client.service_account import ServiceAccountCredentials
from io import FileIO
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import time
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
import os
os.chdir('/home/aryan.rajbhagat/analytics')

#output = subprocess.check_output(['bash', '-c', 'echo $CLASSPATH:/usr/local/fdp-infra-hive/lib/*'])
os.environ['CLASSPATH'] = '/usr/local/fdp-infra-hive/lib/*'

# JDBC connection string
url = ("jdbc:hive2://fkp-fdp-galaxy-sun-zkjn-0001.c.fkp-fdp-galaxy.internal:2181,fkp-fdp-galaxy-sun-zkjn-0002.c.fkp-fdp-galaxy.internal:2181,fkp-fdp-galaxy-sun-zkjn-0003.c.fkp-fdp-galaxy.internal:2181/default;transportMode=http;httpPath=cliservice;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=fkp-fdp-galaxy-hive3-hs2-agni;hive.client.read.socket.timeout=300?hive.metastore.client.socket.timeout=180;hive.server.tcp.keepalive=true;hive.server.read.socket.timeout=300")

# Connect to HiveServer
conn = jaydebeapi.connect("org.apache.hive.jdbc.HiveDriver", url,{'user': "aryan.rajbhagat", 'password': ""})
curs = conn.cursor()
# This is mandatory for running hive queries through jaydebeapi
curs.execute('set hive.cbo.enable=false')

print("Starting")

######################################################################################################################################
#                                                              FDP Query
######################################################################################################################################

print("FDP Query D+1")
### Importing the datasets
cl = """SELECT
    promise_source,
    pincode,
    order_external_id,
    shipment_external_id,
    order_item_fsn,
    fulfill_item_listing_id,
    DATE(item_customer_promised_date) AS icpd,
    COUNT(order_item_id) AS units
FROM bigfoot_external_neo.scp_fulfillment__fulfill_item_unit_live_hourly_fact
WHERE marketplace_domain = 'Grocery'
AND item_delivery_type = 'slotted'
AND DATE(item_customer_promised_date)
    BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), 5)
AND fulfill_item_unit_status NOT IN (
    'customer_cancelled',
    'warehouse_cancellation_requested',
    'seller_cancelled'
)
GROUP BY
    promise_source,
    pincode,
    order_external_id,
    shipment_external_id,
    order_item_fsn,
    fulfill_item_listing_id,
    DATE(item_customer_promised_date)"""

cl = pd.read_sql_query(cl, con=conn)
cl.to_csv('cl1.csv', index=False)

#cl = pd.read_csv('cl1.csv')
print(cl.head())


######################################################################################################################################
#                                                              Stack Merge Pincode Mapping
######################################################################################################################################

print("Reading Pincodes mapping sheet")
### Importing Pincode File
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
cred = ServiceAccountCredentials.from_json_keyfile_name("/home/aryan.rajbhagat/analytics/credentials.json",scope)
client = gspread.authorize(cred)
from oauth2client.service_account import ServiceAccountCredentials

def import_worksheet_to_dataframe(worksheet_name):
    worksheet = client.open(spreadsheet_name).worksheet(worksheet_name)
    data = worksheet.get_all_values()
    df = pd.DataFrame(data[1:], columns=data[0])
    return df

# Importing Segment File
spreadsheet_name = 'pincode_sheet'
worksheet_names = ['stk_pincodes']

# Import all worksheets into separate DataFrames
dataframes = [import_worksheet_to_dataframe(name) for name in worksheet_names]

# Concatenate the DataFrames
stk_pincodes = pd.concat(dataframes, ignore_index=True)

stk_pincodes = stk_pincodes.drop_duplicates(subset=['Pincodes'])

print("mapping hubname to D+1")
#-----Merging HubName
# Ensure the columns are in the correct format for matching
cl['pincode'] = cl['pincode'].astype(str)
stk_pincodes['Pincodes'] = stk_pincodes['Pincodes'].astype(str)

# Merge the two DataFrames on the Pincodes/Pincodes columns
merged_df = cl.merge(stk_pincodes[['Pincodes', 'Hub', 'Route']],
                     left_on='pincode',
                     right_on='Pincodes',
                     how='left')

# Rename the column Hub to Hubname in the merged DataFrame
merged_df = merged_df.rename(columns={'Hub': 'Hubname'})

# Drop the extra Pincodes column if not required
merged_df = merged_df.drop(columns=['Pincodes'])

print(merged_df.head())

######################################################################################################################################
#                                                      FDP Product weight volume
######################################################################################################################################

print("Reading product wt/vol data")
### Importing Product Data
prod = """SELECT *
FROM (
    SELECT
        a.product_id,
        ROW_NUMBER() OVER (PARTITION BY a.product_id) AS number,
        b.product_detail_cms_vertical as cms_vertical,
        b.product_detail_product_title as title,
        b.product_detail_weight,
        b.product_volume
    FROM (
        SELECT DISTINCT
            product_id,
            product_key
        FROM bigfoot_external_neo.scp_ekl__groc_order_item_unit_hive_fact
        WHERE DATE(fulfill_item_unit_reserve_actual_time) >= DATE_SUB(CURRENT_DATE(), 90)
    ) AS a
    LEFT JOIN (
        SELECT DISTINCT
            prd.fc_product_cdm_dim_key,
            prd.product_detail_cms_vertical,
            prd.product_detail_product_title,
            prd.product_detail_weight,
            prd.product_volume
        FROM (
            SELECT
                fc_product_cdm_dim_key,
                product_detail_cms_vertical,
                product_detail_product_title,
                ROW_NUMBER() OVER (
                    PARTITION BY fc_product_cdm_dim_key
                    ORDER BY product_detail_updated_at_timestamp DESC
                ) AS number,
                product_detail_weight,
                (product_detail_length * product_detail_breadth * product_detail_height) AS product_volume
            FROM bigfoot_external_neo.scp_warehouse__fc_product_cdm_dim
            WHERE product_detail_weight NOT IN ('null')
        ) AS prd
        WHERE number = 1
    ) AS b
    ON a.product_key = b.fc_product_cdm_dim_key
) AS c
WHERE number = 1
  AND product_detail_weight NOT IN ('null')"""

prod = pd.read_sql_query(prod, con=conn)
prod.to_csv('prod1.csv', index=False)

# prod = pd.read_csv('prod1.csv')
print(prod.head())

######################################################################################################################################
#                                            Weight volume picking zone addition, total wt, vol calculation
######################################################################################################################################

print("mapping wt/vol")
# Ensure the columns used for merging are of the same type
merged_df['order_item_fsn'] = merged_df['order_item_fsn'].astype(str)
prod['c.product_id'] = prod['c.product_id'].astype(str)

# Merge the two DataFrames on order_item_fsn and product_id
merged_df = merged_df.merge(prod[['c.product_id', 'c.cms_vertical', 'c.title', 'c.product_detail_weight', 'c.product_volume']],
                            left_on='order_item_fsn',
                            right_on='c.product_id',
                            how='left')
                            
merged_df = merged_df.rename(columns={
    'promise_source': 'FC',
    'order_item_fsn': 'fsn',
    'c.product_id': 'product_id',
    'c.cms_vertical': 'cms_vertical',
    'c.title': 'title',
    'c.product_detail_weight': 'product_weight',
    'c.product_volume': 'product_volume'
})

# Drop the redundant product_id column if not needed
merged_df = merged_df.drop(columns=['product_id'])
merged_df = merged_df[merged_df['cms_vertical'].notna() & (merged_df['cms_vertical'] != '')]

# Add calculated columns total_wt and total_vol
merged_df['tot_weight'] = merged_df.apply(
    lambda row: (float(row['product_weight']) if pd.notna(row['product_weight']) else 1) * row['units'],
    axis=1
)

merged_df['tot_volume'] = merged_df.apply(
    lambda row: (float(row['product_volume']) if pd.notna(row['product_volume']) else 100) * row['units'],
    axis=1
)

print("This should be the raw data")
print(merged_df.head())


######################################################################################################################################
#                                                         Separate STk & Others
######################################################################################################################################

print("Separate data for D+1")

# Filter rows where 'Hubname' is non-blank for stk_base
stk_base = merged_df[merged_df['Route'].notna() & (merged_df['Route'] != '')]

stk_base = merged_df[merged_df['Route'].notna() & (merged_df['Route'] != '')]
stk_base.to_csv('raw_data.csv.gz', index=False, compression='gzip')
print("This will be my raw data")
print(stk_base.head())
stk_hub = stk_base.copy()

######################################################################################################################################
#                                                              Risk FC FSN List
######################################################################################################################################
print("Risk FC FSN List")
# Step 1: Identify broken orders
order_tracking_counts = stk_base.groupby('order_external_id')['shipment_external_id'].nunique().reset_index()
broken_orders = order_tracking_counts[order_tracking_counts['shipment_external_id'] > 1]['order_external_id']

# Step 2: Filter for broken orders
broken_snap = stk_base[stk_base['order_external_id'].isin(broken_orders)]
merged_df1 = broken_snap.copy()

# Step 3: Calculate total broken orders per FC and icpd (for penetration%)
total_broken_orders = merged_df1.groupby(['FC', 'icpd'])['order_external_id'].nunique().reset_index()
total_broken_orders = total_broken_orders.rename(columns={'order_external_id': 'total_broken_orders'})

# Step 4: Aggregate FSN-level metrics
t2o_table = (
    merged_df1.groupby(['FC', 'icpd', 'fsn', 'cms_vertical', 'title'])
    .agg(
        units=('units', 'sum'),
        tot_weight=('tot_weight', 'sum'),
        tot_volume=('tot_volume', 'sum'),
        shipment_id_count=('shipment_external_id', pd.Series.nunique),
        order_id_count=('order_external_id', pd.Series.nunique)
    )
    .reset_index()
)

# Step 5: Derived metrics
t2o_table['S2O'] = (t2o_table['shipment_id_count'] / t2o_table['order_id_count']).round(2)

t2o_table['weight_share'] = (
    t2o_table.groupby(['FC', 'icpd'])['tot_weight']
    .transform(lambda x: (x / x.sum()) * 100)
    .round(2)
)

t2o_table['volume_share'] = (
    t2o_table.groupby(['FC', 'icpd'])['tot_volume']
    .transform(lambda x: (x / x.sum()) * 100)
    .round(2)
)

# Step 6: Merge total broken orders and calculate penetration %
t2o_table = t2o_table.merge(total_broken_orders, on=['FC', 'icpd'])
t2o_table['order_penetration_pct'] = (
    (t2o_table['order_id_count'] / t2o_table['total_broken_orders']) * 100
).round(2)

# Step 7: Calculate FC-wise thresholds
thresholds = t2o_table.groupby(['FC', 'icpd']).agg({
    'tot_weight': 'mean',
    'tot_volume': 'mean',
    'units': 'mean',
    'S2O': 'mean',
    'order_id_count': 'mean'
}).rename(columns={
    'tot_weight': 'avg_weight',
    'tot_volume': 'avg_volume',
    'units': 'avg_units',
    'S2O': 'avg_S2O',
    'order_id_count': 'avg_order_count'
}).reset_index()

# Step 8: Merge thresholds and filter based on above-average values
t2o_table = t2o_table.merge(thresholds, on=['FC', 'icpd'])
t2o_table = t2o_table[
    (t2o_table['tot_weight'] > t2o_table['avg_weight']) &
    (t2o_table['tot_volume'] > t2o_table['avg_volume']) &
    (t2o_table['units'] > t2o_table['avg_units']) &
    (t2o_table['S2O'] > t2o_table['avg_S2O']) &
    (t2o_table['order_id_count'] > t2o_table['avg_order_count'])
]

# Step 9: Final formatting
final_table = t2o_table[[
    'FC', 'icpd', 'fsn', 'units', 'cms_vertical', 'title',
    'tot_weight', 'tot_volume', 'order_id_count', 'shipment_id_count',
    'S2O', 'weight_share', 'volume_share', 'order_penetration_pct'
]]

# Step 10: Sort and select top 50 FSNs per FC and icpd
final_table = final_table.sort_values(
    by=['FC', 'icpd', 'weight_share', 'S2O', 'order_id_count', 'units'],
    ascending=[True, True, False, False, False, False]
)

final_table = final_table.groupby(['FC', 'icpd']).head(50).reset_index(drop=True)

# Step 11: Export to CSV
final_table.to_csv("FC_FSN_List.csv", index=False)
print(final_table.head())


######################################################################################################################################
#                                                              Risk Route FSN List
######################################################################################################################################

print("Risk Route FSN List")
# Step 1: Identify broken orders
order_tracking_counts = stk_base.groupby('order_external_id')['shipment_external_id'].nunique().reset_index()
broken_orders = order_tracking_counts[order_tracking_counts['shipment_external_id'] > 1]['order_external_id']

# Step 2: Filter for broken orders
broken_snap = stk_base[stk_base['order_external_id'].isin(broken_orders)]
merged_df1 = broken_snap.copy()

# Step 3: Calculate total broken orders per Route and icpd (for penetration%)
total_broken_orders = merged_df1.groupby(['Route', 'icpd'])['order_external_id'].nunique().reset_index()
total_broken_orders = total_broken_orders.rename(columns={'order_external_id': 'total_broken_orders'})

# Step 4: Aggregate FSN-level metrics
t2o_table = (
    merged_df1.groupby(['FC', 'Route', 'icpd', 'fsn', 'cms_vertical', 'title'])
    .agg(
        units=('units', 'sum'),
        tot_weight=('tot_weight', 'sum'),
        tot_volume=('tot_volume', 'sum'),
        shipment_id_count=('shipment_external_id', pd.Series.nunique),
        order_id_count=('order_external_id', pd.Series.nunique)
    )
    .reset_index()
)

# Step 5: Derived metrics
t2o_table['S2O'] = (t2o_table['shipment_id_count'] / t2o_table['order_id_count']).round(2)

t2o_table['weight_share'] = (
    t2o_table.groupby(['Route', 'icpd'])['tot_weight']
    .transform(lambda x: (x / x.sum()) * 100)
    .round(2)
)

t2o_table['volume_share'] = (
    t2o_table.groupby(['Route', 'icpd'])['tot_volume']
    .transform(lambda x: (x / x.sum()) * 100)
    .round(2)
)

# Step 6: Merge total broken orders and calculate penetration %
t2o_table = t2o_table.merge(total_broken_orders, on=['Route', 'icpd'])
t2o_table['order_penetration_pct'] = (
    (t2o_table['order_id_count'] / t2o_table['total_broken_orders']) * 100
).round(2)

# Step 7: Calculate Route-wise thresholds
thresholds = t2o_table.groupby(['Route', 'icpd']).agg({
    'tot_weight': 'mean',
    'tot_volume': 'mean',
    'units': 'mean',
    'S2O': 'mean',
    'order_id_count': 'mean'
}).rename(columns={
    'tot_weight': 'avg_weight',
    'tot_volume': 'avg_volume',
    'units': 'avg_units',
    'S2O': 'avg_S2O',
    'order_id_count': 'avg_order_count'
}).reset_index()

# Step 8: Merge thresholds and filter based on above-average values
t2o_table = t2o_table.merge(thresholds, on=['Route', 'icpd'])
t2o_table = t2o_table[
    (t2o_table['tot_weight'] > t2o_table['avg_weight']) &
    (t2o_table['tot_volume'] > t2o_table['avg_volume']) &
    (t2o_table['units'] > t2o_table['avg_units']) &
    (t2o_table['S2O'] > t2o_table['avg_S2O']) &
    (t2o_table['order_id_count'] > t2o_table['avg_order_count'])
]

# Step 9: Final formatting
final_table = t2o_table[[
    'FC', 'Route', 'icpd', 'fsn', 'units', 'cms_vertical', 'title',
    'tot_weight', 'tot_volume', 'order_id_count', 'shipment_id_count',
    'S2O', 'weight_share', 'volume_share', 'order_penetration_pct'
]]

# Step 10: Sort and select top 50 FSNs per Route and icpd
final_table = final_table.sort_values(
    by=['FC', 'Route', 'icpd', 'weight_share', 'S2O', 'order_id_count', 'units'],
    ascending=[True, True, True, False, False, False, False]
)

final_table = final_table.groupby(['Route', 'icpd']).head(10).reset_index(drop=True)

# Step 11: Export to CSV
final_table.to_csv("Route_FSN_List.csv", index=False)
print(final_table.head())


######################################################################################################################################
#                                                              Risk Pincode FSN List
######################################################################################################################################

print("Risk Pincode FSN List")
# Step 1: Identify broken orders
order_tracking_counts = stk_base.groupby('order_external_id')['shipment_external_id'].nunique().reset_index()
broken_orders = order_tracking_counts[order_tracking_counts['shipment_external_id'] > 1]['order_external_id']

# Step 2: Filter for broken orders
broken_snap = stk_base[stk_base['order_external_id'].isin(broken_orders)]
merged_df1 = broken_snap.copy()

# Step 3: Calculate total broken orders per Pincode and icpd (for penetration%)
total_broken_orders = merged_df1.groupby(['pincode', 'icpd'])['order_external_id'].nunique().reset_index()
total_broken_orders = total_broken_orders.rename(columns={'order_external_id': 'total_broken_orders'})

# Step 4: Aggregate FSN-level metrics
t2o_table = (
    merged_df1.groupby(['FC', 'Route', 'pincode', 'icpd', 'fsn', 'cms_vertical', 'title', 'fulfill_item_listing_id'])
    .agg(
        units=('units', 'sum'),
        tot_weight=('tot_weight', 'sum'),
        tot_volume=('tot_volume', 'sum'),
        shipment_id_count=('shipment_external_id', pd.Series.nunique),
        order_id_count=('order_external_id', pd.Series.nunique)
    )
    .reset_index()
)

# Step 5: Derived metrics
t2o_table['S2O'] = (t2o_table['shipment_id_count'] / t2o_table['order_id_count']).round(2)

t2o_table['weight_share'] = (
    t2o_table.groupby(['pincode', 'icpd'])['tot_weight']
    .transform(lambda x: (x / x.sum()) * 100)
    .round(2)
)

t2o_table['volume_share'] = (
    t2o_table.groupby(['pincode', 'icpd'])['tot_volume']
    .transform(lambda x: (x / x.sum()) * 100)
    .round(2)
)

# Step 6: Merge total broken orders and calculate penetration %
t2o_table = t2o_table.merge(total_broken_orders, on=['pincode', 'icpd'])
t2o_table['order_penetration_pct'] = (
    (t2o_table['order_id_count'] / t2o_table['total_broken_orders']) * 100
).round(2)

# Step 7: Calculate pincode-wise thresholds
thresholds = t2o_table.groupby(['pincode', 'icpd']).agg({
    'tot_weight': 'mean',
    'tot_volume': 'mean',
    'units': 'mean',
    'S2O': 'mean',
    'order_id_count': 'mean'
}).rename(columns={
    'tot_weight': 'avg_weight',
    'tot_volume': 'avg_volume',
    'units': 'avg_units',
    'S2O': 'avg_S2O',
    'order_id_count': 'avg_order_count'
}).reset_index()

# Step 8: Merge thresholds and filter based on above-average values
t2o_table = t2o_table.merge(thresholds, on=['pincode', 'icpd'])
t2o_table = t2o_table[
    (t2o_table['tot_weight'] > t2o_table['avg_weight']) &
    (t2o_table['tot_volume'] > t2o_table['avg_volume']) &
    (t2o_table['units'] > t2o_table['avg_units']) &
    (t2o_table['S2O'] > t2o_table['avg_S2O']) &
    (t2o_table['order_id_count'] > t2o_table['avg_order_count'])
]

# Step 9: Final formatting
final_table = t2o_table[[
    'FC', 'Route', 'pincode', 'icpd', 'fsn', 'title', 'fulfill_item_listing_id', 'units', 'cms_vertical',
    'tot_weight', 'tot_volume', 'order_id_count', 'shipment_id_count',
    'S2O', 'weight_share', 'volume_share', 'order_penetration_pct'
]]

# Step 10: Sort and select top 3 FSNs per pincode and icpd
final_table = final_table.sort_values(
    by=['FC', 'Route', 'icpd', 'weight_share', 'S2O', 'order_id_count', 'units'],
    ascending=[True, True, True, False, False, False, False]
)

final_table = final_table.groupby(['pincode', 'icpd']).head(3).reset_index(drop=True)

# Step 11: Export to CSV
final_table.to_csv("Pincode_FSN_List.csv", index=False)
print(final_table.head())

######################################################################################################################################
#                                                              Top 15 FSN List
######################################################################################################################################

# Step 1: Load both FSN lists
fc_fsn_df = pd.read_csv("FC_FSN_List.csv")
route_fsn_df = pd.read_csv("Route_FSN_List.csv")

# Step 2: Add source tags
fc_fsn_df['source'] = 'FC'
route_fsn_df['source'] = 'Route'

# Step 3: Normalize to same schema for union (keeping only relevant columns)
fc_fsn_reduced = fc_fsn_df[['FC', 'icpd', 'fsn', 'source', 'units', 'tot_weight', 'tot_volume', 'order_id_count']]
route_fsn_reduced = route_fsn_df[['FC', 'icpd', 'fsn', 'source', 'units', 'tot_weight', 'tot_volume', 'order_id_count']]

# Step 4: Combine and score
combined_df = pd.concat([fc_fsn_reduced, route_fsn_reduced], ignore_index=True)

# Assign score: 2 if FSN appears in both, else 1
fsn_score = (
    combined_df.groupby(['FC', 'fsn'])
    .agg(
        icpd=('icpd', 'first'),
        score=('source', lambda x: 2 if len(set(x)) > 1 else 1),
        total_units=('units', 'sum'),
        total_weight=('tot_weight', 'sum'),
        total_volume=('tot_volume', 'sum'),
        total_orders=('order_id_count', 'sum')
    )
    .reset_index()
)

# Step 5: Sort by score, then other metrics
fsn_score = fsn_score.sort_values(
    by=['FC', 'score', 'total_orders', 'total_units', 'total_weight'],
    ascending=[True, False, False, False, False]
)

# Step 6: Take top 15 FSNs per FC
top_fsn_per_fc = fsn_score.groupby('FC').head(15).reset_index(drop=True)

# Step 7: Export final list
top_fsn_per_fc.to_csv("Top_FSN.csv", index=False)
print(top_fsn_per_fc.head(20))

msg = MIMEMultipart()


html = """
<html>
  <head></head>
  <body>
    <p>Risk FSN List for upcoming 5 days<br>
    </p>
  </body>
</html>
"""
part1 = MIMEText(html, 'html')
msg.attach(part1)

two_Days_ago = datetime.now() - timedelta(days=2)
formatted_date = two_Days_ago.strftime("%Y-%m-%d")

files = ['FC_FSN_List.csv', 'Route_FSN_List.csv', 'Pincode_FSN_List.csv', 'Top_FSN.csv']

for a_file in files:
    attachment = open(a_file, 'rb')
    file_name = os.path.basename(a_file)
    part = MIMEBase('application','octet-stream')
    part.set_payload(attachment.read())
    part.add_header('Content-Disposition',
                    'attachment',
                    filename=file_name)
    encoders.encode_base64(part)
    msg.attach(part)

fromaddr = "grocery_ops_analytics@flipkart.com"
#toaddr = ["aryan.rajbhagat@flipkart.com", "harshal.dalvi@flipkart.com", "grocery_ops_analytics@flipkart.com", "grocery-ct-managers@flipkart.com"]
toaddr = ["aryan.rajbhagat@flipkart.com"]


# storing the senders email address
msg['From'] = fromaddr

# storing the receivers email address

msg['To'] = ", ".join(toaddr)

two_Days_ago = datetime.now()
formatted_date = two_Days_ago.strftime("%Y-%m-%d")

# storing the subject
msg['Subject'] = f"Risk FSN List - {formatted_date}"

# creates SMTP session
s = smtplib.SMTP('127.0.0.1')

# Converts the Multipart msg into a string
text = msg.as_string()

# sending the mail
s.sendmail(fromaddr, toaddr, text)

print('mail sent')

# terminating the session
s.quit()

curs.close()