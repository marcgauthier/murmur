When schema is declare:
  Primary Keys: Every replicated table requires an explicit BLOB(16) primary key (e.g. replicateddb.NewRowID()). SQLite rowid /         
  autoincrement is not replicated.
  - this should be done automatically the user must flag a column name:
        if no column is selected the Primary Keys is a UUIDv4 column name id
        if column is selected the Primary Keys is a UUIDv5 (tablename + colunmn name value)
        
        

