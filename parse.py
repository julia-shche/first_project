import re
from geolite2 import geolite2
import sqlalchemy as db
from database import insert_country_and_visit, insert_pay, insert_cart, insert_history_goods


# Группировка по ip и date (действия одного пользователя)
def group(data, start):
    data_copy = data.copy()
    temp_date = data_copy['date'][data.index[start]]
    temp_ip_address = data_copy['ip_address'][data.index[start]]
    row_count = start

    i = start
    while (i < data_copy.shape[0] and data['date'][data.index[i]] == temp_date) and (data['ip_address'][data.index[i]] == temp_ip_address ):
        row_count += 1
        i += 1

    action_list = data[start:row_count]
    index = row_count
    return action_list, index
	

# Парсер
def parse_url(url):
    # Можно разделить на 4 подгруппы:
    # 0 - просмотр категории / категории и товара
    # 1 - добавление товара в корзину
    # 2 - Оплата
    # 3 - Успешная оплата

    if 'cart?' in url:
        result = re.findall(r'([0-9]+)', url)  # result = [goods_id, amount, cart_id]
        key = 1
    elif 'pay?' in url:
        result = re.findall(r'([0-9]+)', url)  # result = [user_id, cart_id]
        key = 2
    elif 'success_pay' in url:
        result = re.findall(r'([0-9]+)', url)  # result = [success_pay]
        key = 3
    else:
        result = re.split(r'/', url)  # result = [category] / [category, goods]
        key = 0
    return key, result
	
	
# Определение страны по ip
def country(ip):
    reader = geolite2.reader()
    result = reader.get(ip)
    if result != None:
        try:
            country_name = result['country']["names"]["en"]
        except KeyError:
            country_name = result['registered_country']["names"]["en"]
    else:
        country_name = ''    
    return country_name
	
	
# Получение нужной информации 
def filling_in_tables(logs, database):
    logs_copy = logs.copy()

    start = 0  
    id_v = 0 
    id_h = 0 
    id_c = 0 
    id_p = 0 
    id_con = 0 
    id_go = 0

    while start < logs_copy.shape[0]: 
        group_logs, start = group(logs_copy, start)
        id_v += 1 # id (table_visit)
        id_con += 1
        date_v = group_logs['date'][group_logs.index[0]] # дата (table_visit)
        ip_v = group_logs['ip_address'][group_logs.index[0]] # ip (table_vsit)
        country_v = country(logs_copy['ip_address'][logs_copy.index[start-1]]) # страна (table_country)
        insert_country_and_visit(database, id_con, country_v, id_v, date_v, ip_v)

        for j in range(group_logs.shape[0]):
            s = group_logs['url'][group_logs.index[j]]
            s = re.sub(r'https://all_to_the_bottom.com/', '', s)
	
            if s != '':
                key, inf = parse_url(s)

                # 0 - просмотр категории / категории и товара  
                # inf = [category] / [category, goods]
                if key == 0: 
                    id_h += 1
                    id_go += 1
                    if len(inf) == 1:
                        category_h = inf[0] # категория (table_history)
                        goods_h  = '' # товар (table_histiry)
                    else:
                        category_h = inf[0] # категория (table_history)
                        goods_h = inf[1] # товар (table_histiry)
                    time_h = group_logs['time'][group_logs.index[j]] # время (table_histiry)
                    insert_history_goods(database, id_h, id_go, time_h, id_v, goods_h, category_h)

                # 1 - добавление товара в корзину
                # inf = [goods_id, amount, cart_id]
                elif key == 1:
                    id_c += 1
                    goods_id_g = inf[0] # id товара (table_cart)
                    amount_c = inf[1] # кол-во товара (table_cart)
                    cart_number = inf[2] # (table_cart)
                    time_c = group_logs['time'][group_logs.index[j]] #(table_cart)
                    insert_cart(database, id_c, time_c, amount_c, cart_number, id_v, id_h)
					
                # 2 - Оплата
                # inf = [user_id, cart_id]
                elif key == 2:
                    good_id = inf[0]
                    cart_id = inf[1]

                # 3 - Успешная оплата
                # inf = [success_pay]
                elif key == 3:
                    id_p += 1
                    time_p = group_logs['time'][group_logs.index[j]] # время (table_pay)
                    pay_number = inf[0] #(table_pay)
                    insert_pay(database, id_p, pay_number, time_p, id_v)

