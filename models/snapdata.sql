CREATE SCHEMA IF NOT EXISTS "snapdata";

CREATE  TABLE "snapdata".dim_address ( 
	address_id           integer  NOT NULL  ,
	street_number        integer    ,
	street_name          varchar(50)    ,
	additional_address   varchar(50)    ,
	city                 varchar(20)    ,
	"state"              varchar(20)    ,
	zip_code             integer    ,
	zip4                 integer    ,
	county               varchar(20)    ,
	CONSTRAINT pk_dim_address PRIMARY KEY ( address_id )
 );

CREATE  TABLE "snapdata".dim_store ( 
	record_id            integer  NOT NULL  ,
	store_name           varchar(100)    ,
	authorization_year   integer    ,
	authorization_data   date    ,
	end_data             date    ,
	CONSTRAINT pk_dim_store PRIMARY KEY ( record_id )
 );

CREATE  TABLE "snapdata".dim_storetype ( 
	storetype_id         varchar(2)  NOT NULL  ,
	store_type           varchar(100)    ,
	CONSTRAINT pk_dim_storetype PRIMARY KEY ( storetype_id )
 );

CREATE  TABLE "snapdata".fact_snap ( 
	record_id            integer    ,
	storetype_id         varchar(2)    ,
	address_id           integer    ,
	latitude             double precision    ,
	longitude            double precision    ,
	CONSTRAINT pk_address_id UNIQUE ( address_id ) ,
	CONSTRAINT pk_record_id UNIQUE ( record_id ) ,
	CONSTRAINT pk_storetype_id UNIQUE ( storetype_id ) 
 );

ALTER TABLE "snapdata".fact_snap ADD CONSTRAINT fk_fact_snap_dim_store FOREIGN KEY ( record_id ) REFERENCES "snapdata".dim_store( record_id );

ALTER TABLE "snapdata".fact_snap ADD CONSTRAINT fk_fact_snap_dim_storetype FOREIGN KEY ( storetype_id ) REFERENCES "snapdata".dim_storetype( storetype_id );

ALTER TABLE "snapdata".fact_snap ADD CONSTRAINT fk_fact_snap_dim_address FOREIGN KEY ( address_id ) REFERENCES "snapdata".dim_address( address_id );
