--SQL Ingestion of core dataset
WbVarDef varBucket = 's3://' --Add Path
WbVarDef varIAMRole = '' --Add IAM Role

WbVarDef varSchema = 'safegraph' --Add Schema 
WbVarDef varTable = 'core' --Define table name 


WbVarDef currentFilePath = ''$[varBucket]core_part1.csv.gz'';--Add Table Name 


DROP TABLE IF EXISTS $[varSchema].$[varTable] CASCADE;
CREATE TABLE $[varSchema].$[varTable](
		placekey varchar(19) NULL
		,parent_placekey varchar(19) NULL
		,location_name varchar(210) NULL
		,safegraph_brand_ids varchar(41) NULL
		,brands varchar(256) NULL
		,top_category varchar(100) NULL
		,sub_category varchar(115) NULL
		,naics_code varchar(6) NULL
		,latitude double NULL
		,longitude double NULL
		,street_address varchar(150) NULL
		,city varchar(50) NULL
		,region varchar(40) NULL
		,postal_code varchar(8) NULL
		,iso_country_code varchar(2) NULL
		,phone_number varchar(13) NULL
		,open_hours varchar(460) NULL
		,category_tags varchar(125) NULL
		,opened_on date NULL
		,closed_on date NULL
		,tracking_closed_since date NULL
)
DISTKEY(placekey)
COMPOUND SORTKEY(placekey, safegraph_brand_ids, opened_on)
;

COPY $[varSchema].$[varTable] FROM $[currentFilePath] IAM_ROLE '$[varIAMRole]'  IGNOREHEADER 1 DATEFORMAT 'YYYY-MM-DD' ACCEPTINVCHARS CSV;



--SQL Ingestion of Geometry Dataset 

WbVarDef varBucket = 's3://' --Add Path
WbVarDef varIAMRole = '' --Add IAM Role

WbVarDef varSchema = 'safegraph' --Add Schema 
WbVarDef varTable = 'geometry' --Define table name 



WbVarDef currentFilePath = ''$[varBucket]geometry_part1.csv.gz'';--Add Table Name 


DROP TABLE IF EXISTS $[varSchema].$[varTable] CASCADE;
CREATE TABLE $[varSchema].$[varTable](
		placekey varchar(19) NULL
		,parent_placekey varchar(19) NULL
		,location_name varchar(210) NULL
		,brands varchar(256) NULL
		,latitude double NULL
		,longitude double NULL
		,street_address varchar(150) NULL
		,city varchar(50) NULL
		,region varchar(40) NULL
		,postal_code varchar(8) NULL
		,iso_country_code varchar(2) NULL
		,polygon_wkt varchar(MAX) NULL
		,polygon_class varchar(14) NULL
		,includes_parking_lot boolean NULL
		,is_synthetic boolean NULL
		,building_height int NULL
		,enclosed boolean NULL
)
DISTKEY(placekey)
COMPOUND SORTKEY(placekey)
;

COPY $[varSchema].$[varTable] FROM $[currentFilePath] IAM_ROLE '$[varIAMRole]'  IGNOREHEADER 1 ACCEPTINVCHARS CSV;


--SQL Ingestion of Patterns Dataset 

WbVarDef varBucket = 's3://' --Add Path
WbVarDef varIAMRole = '' --Add IAM Role

WbVarDef varSchema = 'safegraph' --Add Schema 
WbVarDef varTable = 'patterns' --Define table name 



WbVarDef currentFilePath = ''$[varBucket]patterns_part1.csv.gz'';--Add Table Name 


DROP TABLE IF EXISTS $[varSchema].$[varTable] CASCADE;
CREATE TABLE $[varSchema].$[varTable](
		placekey varchar(19) NULL
		,parent_placekey varchar(19) NULL
		,location_name varchar(210) NULL
		,street_address varchar(150) NULL
		,city varchar(50) NULL
		,region varchar(40) NULL
		,postal_code varchar(8) NULL
		,safegraph_brand_ids varchar(41) NULL
		,brands varchar(256) NULL
		,date_range_start timestamp NULL
		,date_range_end timestamp NULL
		,raw_visit_counts int NULL
		,raw_visitor_counts int NULL
		,visits_by_day varchar(202) NULL
		,poi_cbg varchar(12) NULL
		,visitor_home_cbgs varchar(MAX) NULL
		,visitor_home_aggregation varchar(MAX) NULL
		,visitor_daytime_cbgs varchar(MAX) NULL
		,visitor_country_of_origin varchar(821) NULL
		,distance_from_home int NULL
		,median_dwell int NULL
		,bucketed_dwell_times varchar(110) NULL
		,related_same_day_brand varchar(510) NULL
		,related_same_month_brand varchar(600) NULL
		,popularity_by_hour varchar(175) NULL
		,popularity_by_day varchar(121) NULL
		,device_type varchar(31) NULL
		,carrier_name varchar(95) NULL
)
DISTKEY(placekey)
COMPOUND SORTKEY(placekey, safegraph_brands_ids, data_range_start)
;

COPY $[varSchema].$[varTable] FROM $[currentFilePath] IAM_ROLE '$[varIAMRole]'  IGNOREHEADER 1 ACCEPTINVCHARS CSV;


