# модуль собирает суммарную базу данных ZDPress из бэкапов разных периодов.
# эти бэкапы предварительно загружаются в БД и переименовываются по принципу ZDPress1, ZDPress2 и т.п.
# после загрузки кусков бэкапов удалаются дубликаты и база записывается.
#
# если программа работает, то её писал Сергей Симкович.
# а если не работает, то не знаю, кто писал.
#
import pandas as pd
import sqlalchemy as sal
import datetime

ZD_operations = pd.DataFrame()
ZD_data = pd.DataFrame()
start = datetime.datetime.now()
begin = start
print(start, "Этап 1. Запуск цикла")
for x in range(5):
    engine_text = 'mssql+pyodbc://Black:N@noporox@SERVER10/ZDPress' + str(x+1) + '?driver=ODBC+Driver+17+for+SQL+Server'
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
