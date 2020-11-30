import pandas as pd
import sqlalchemy as sal

# открываем базу-источник с пресса
engine_text = 'mssql+pyodbc://Black:N@noporox@192.168.1.201/ZDPress?driver=ODBC+Driver+17+for+SQL+Server'
base = sal.create_engine(engine_text)
connector = base.connect()
ZD_operations = pd.read_sql_query('select * from dbo.PressOperations', connector)
ZD_data = pd.read_sql_query('select * from dbo.PressOperationData', connector)
connector.close()


print(ZD_operations.info(verbose=False))
print(ZD_data.info(verbose=False))