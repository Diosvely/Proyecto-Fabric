CREATE TABLE [Gold].[fact_sitios] (

	[Id_fact_sitios] bigint NOT NULL, 
	[FK_Id_municipio] int NULL, 
	[FK_Id_tipo] int NULL, 
	[FK_Id_modalidad] int NULL, 
	[FK_Id_codigopostal] int NULL, 
	[nombre] varchar(max) NULL, 
	[direccion] varchar(max) NULL, 
	[aforo_interior] bigint NULL, 
	[aforo_terraza] bigint NULL
);