import pandas as pd

wheels = pd.read_csv('/home/ftp/press5.csv')
trash = []                                                                      # датафрейм для инвалидов

# первичный анализ, сбор статистики и отсев явного брака
counter_2 = counter_3 = counter_4 = counter_5 = counter_6 = other = absent = 0
for key, x in wheels.iterrows():
    if x['filename'][0] == '-':     # нет номера оси? инвалид
        absent += 1
        # убрать такую строку
        wheels.drop(key, axis=0, inplace=True)
        # записать к инвалидам
        string_to_add = [x['filename'], 'Отсутствует номер оси']
        trash.append(string_to_add)
        continue
    qty = x['filename'].count('-')
    if qty == 2:
        counter_2 += 1
    elif qty == 3:
        counter_3 += 1
    elif qty == 4:
        counter_4 += 1
    elif qty == 5:
        counter_5 += 1
    elif qty == 6:
        counter_6 += 1
    else:
        other += 1
        # либо дефисов меньше 2, либо вообще не пойми что. удалить.
        wheels.drop(key, axis=0, inplace=True)
        # записать к инвалидам
        string_to_add = [x['filename'], 'Ошибочная запись. Проверить вручную.']
        trash.append(string_to_add)

print('')
print('Всего в обработке: ' + str(len(wheels)) + ', в том числе:')
print('С нехваткой данных:', absent)
print('2 разделителя:', counter_2)
print('3 разделителя:', counter_3)
print('4 разделителя:', counter_4)
print('5 разделителей:', counter_5)
print('6 разделителей:', counter_6)
print('Иное:', other)

# анализ глубже. парсинг построчно и проверка значений
for key, x in wheels.iterrows():
    if x['filename'].count('-') > 2:
        string = x['filename']
        pos = string.find('-')                              # позиция первого дефиса
        axis = string[:pos]                                 # номер оси, срез от начала до первого дефиса
        axis_year = string[pos+1:string.find('-', pos+1)]   # год оси, срез от дефиса вперёд до второго дефиса
        # нужна проверка. иногда год пропущен, и тогда на его месте посторонние данные. инвалид!
        if len(axis_year) > 2:
            # убрать такую строку
            wheels.drop(key, axis=0, inplace=True)
            # записать к инвалидам
            string_to_add = [x['filename'], 'Отсутствует год изготовления оси']
            trash.append(string_to_add)

        #TODO посчитать позицию, откуда начинается номер ЦКК
        #TODO посчитать позицию, где заканчивается номер ЦКК
        #wheel = string[len(axis_year)+len(axis_year)+1:]
trashDF = pd.DataFrame(trash, columns=['filename', 'error description'])
print(trashDF)
print('')
print(wheels.info(verbose=None))