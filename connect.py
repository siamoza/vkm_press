import pandas as pd
import sqlalchemy as sal

# открываем базу-источник из развернутого бэкапа
backup = sal.create_engine('mssql+pyodbc://Black:N@noporox@SERVER10/ZDPress?driver=ODBC+Driver+17+for+SQL+Server')
conn_bck = backup.connect()
press_src_operations = pd.read_sql_query('select * from dbo.PressOperations', conn_bck)
press_src_data = pd.read_sql_query('select * from dbo.PressOperationData', conn_bck)

# открываем базу-приёмник
engine = sal.create_engine('mssql+pyodbc://Black:N@noporox@SERVER10/ZDPress1?driver=ODBC+Driver+17+for+SQL+Server')
conn_dst = engine.connect()

# перенос первой таблицы
conn_dst.execute("SET IDENTITY_INSERT PressOperations ON")
press_src_operations.to_sql('PressOperations', conn_dst, index=False, if_exists='append')
conn_dst.execute("SET IDENTITY_INSERT PressOperations OFF")

# перенос второй таблицы
conn_dst.execute("SET IDENTITY_INSERT PressOperationData ON")
press_src_data.to_sql('PressOperationData', conn_dst, index=False, if_exists='append')
conn_dst.execute("SET IDENTITY_INSERT PressOperationData OFF")

# закрываем
conn_bck.close()
conn_dst.close()
