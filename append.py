# Модуль сначала читает базу данных из бэкапа, который предварительно развёрнут на SERVER10.
# База ZDPress, таблицы PressOperations и PressOperationData.
# Затем читает существующую БД ZDPress1, те же две таблицы.
# Далее аппендит таблицу из певой базы с аналогичной таблицей из второй, т.е.:
# ZDPress1/PressOperations <append> ZDPress/PressOperations
# ZDPress1/PressOperationData <append> ZDPress/PressOperationData
# Далее делается дедупликация по склеенным таблицам и запись обновленных таблиц в ZDPress1.
# (вместо ZDPress1 может быть ZDPress3, если речь о базе с Пресса3)
#
# И если эта программа работает, то её писал Сергей Симкович.
# А если не работает, то не знаю, кто писал.
#
import pandas as pd
import sqlalchemy as sal
import datetime

ZD_operations = pd.DataFrame()
ZD_data = pd.DataFrame()
start = datetime.datetime.now()
begin = start
# место, где редактируются параметры скрипта
USER = 'Black'
PASSWORD = 'N@noporox'
SERVER = 'SERVER10'
PRESS = '1'   # константа-номер пресса
#
print(start, "Этап 1. Чтение бэкапа")
engine_text = 'mssql+pyodbc://' + USER + ':' + PASSWORD + '@' + SERVER + '/ZdPress?driver=ODBC+Driver+17+for+SQL+Server'
base = sal.create_engine(engine_text)
connector = base.connect()
operations = pd.read_sql_query('select * from dbo.PressOperations', connector)
data = pd.read_sql_query('select * from dbo.PressOperationData', connector)
ZD_operations = pd.concat([ZD_operations, operations])
ZD_data = pd.concat([ZD_data, data])
connector.close()

now = datetime.datetime.now()
delta = now - start
start = now
print(delta, "Выполнен шаг", x, " --------------------------------------")
print(operations.info())
print(data.info())
print("Суммарные таблицы:")
print(ZD_operations.info())
print(ZD_data.info())

now = datetime.datetime.now()
delta = now - start
start = now
print(delta, "Цикл завершён.")
print("Этап 2. Дедупликация --------------------------------------")
print("Размеры таблиц ДО дедупликации:")
print(ZD_operations.info())
print(ZD_data.info())
print("Дедупликация:")
ZD_operations.drop_duplicates("ID", inplace=True)
now = datetime.datetime.now()
delta = now - start
start = now
print(delta, "ZD_operation... завершена!")
ZD_data.drop_duplicates("ID", inplace=True)
now = datetime.datetime.now()
delta = now - start
start = now
print(delta, "ZD_data... завершена!")
print("Размеры таблиц ПОСЛЕ дедупликации:")
print(ZD_operations.info())
print(ZD_data.info())

print("Этап 3. Перенос чистых таблиц в SQL --------------------------------------")
# открываем суммарную базу-приёмник
engine_dst = sal.create_engine('mssql+pyodbc://Black:N@noporox@SERVER10/ZDPress?driver=ODBC+Driver+17+for+SQL+Server')
connector_dst = engine_dst.connect()

# перенос PressOperations в приёмник
now = datetime.datetime.now()
delta = now - start
start = now
print(delta, "ZD_operation... начало переноса --------------------------------------")
connector_dst.execute("SET IDENTITY_INSERT PressOperations ON")
ZD_operations.to_sql('PressOperations', connector_dst, index=False, if_exists='append')
connector_dst.execute("SET IDENTITY_INSERT PressOperations OFF")
print("ZD_operation... перенос окончен --------------------------------------")

# перенос PressOperationData в приёмник
now = datetime.datetime.now()
delta = now - start
start = now
print(delta, "ZD_data... начало переноса --------------------------------------")
connector_dst.execute("SET IDENTITY_INSERT PressOperationData ON")
ZD_data.to_sql('PressOperationData', connector_dst, index=False, if_exists='append')
connector_dst.execute("SET IDENTITY_INSERT PressOperationData OFF")
print("ZD_data... перенос окончен --------------------------------------")

# закрытие суммарной базы
connector_dst.close()
now = datetime.datetime.now()
delta = now - begin
print(delta, "Скрипт завершён.")
