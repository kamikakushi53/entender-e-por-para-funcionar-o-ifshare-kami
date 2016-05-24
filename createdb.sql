CREATE TABLE 'arquivos' 
  ( 
     'sha1arquivo' VARCHAR(40) NOT NULL, 
     'nome'        VARCHAR(128) NOT NULL, 
     'tamanho'     BIGINT NOT NULL, 
     'datahora'    DATETIME NOT NULL, 
     'chunksize'   INT NOT NULL, 
     'numchunks'   INT NOT NULL 
  ) 
engine = innodb; 

CREATE TABLE 'partes' 
  ( 
     'sha1arquivo' VARCHAR(40) NOT NULL, 
     'sha1parte'   VARCHAR(40) NOT NULL, 
     'chunk'       INT NOT NULL, 
     'onde'        VARCHAR(512) NOT NULL 
  ) 
engine = innodb; 
