from sqlalchemy import ForeignKey
from sqlalchemy import Integer, String, Boolean, Date
from sqlalchemy import MetaData
from sqlalchemy import Table, Column
from sqlalchemy import create_engine
import sqlalchemy as db


# Создание таблиц базы данных online_store
def database_creation():
    engine = create_engine('sqlite:///online_store.db', echo=True)
    metadata = MetaData(bind=engine)

    # Создание таблиц
    # Пользователь 
    visit_table = Table(
                'Visit', metadata,
                Column('id_v', Integer, primary_key=True),
                Column('date', String, nullable=False),
                Column('ip', String(20), nullable=False),
                Column('contry_id', Integer, ForeignKey('Country.c_id'))
            )
    
    # Страна
    country_table = Table(
                'Country', metadata,
                Column('c_id', Integer, primary_key=True),
                Column('name_country', String(50), nullable=True)
            )

    # Корзина
    cart_table = Table(
                'Cart', metadata,
                Column('id_c', Integer, primary_key=True),
                Column('amount', String(5), nullable=False),
                Column('time', String(10), nullable=False),
                Column('visit', Integer, ForeignKey('Visit.id_v')),
                Column('cart_number', String(10), ForeignKey('Pay.number_pay')),
                Column('cat_goods', Integer,  ForeignKey('History.id_h'))
            )

    # Покупка товара 
    pay_table = Table(
                'Pay', metadata,
				Column('id_p', Integer, primary_key=True),
                Column('number_pay', String),
                Column('time', String(10), nullable=False),
                Column('visit', Integer, ForeignKey('Visit.id_v'))
            )

    # История просмотра
    history_table = Table(
                'History', metadata,
                Column('id_h', Integer, primary_key=True),
                Column('id_goods', Integer, ForeignKey('Goods.id_g')),
                Column('category', String),
                Column('time', String(10), nullable=False),
                Column('visit', Integer, ForeignKey('Visit.id_v'))
            )

    # Товар
    goods_table = Table(
                'Goods', metadata,
                Column('id_g', Integer, primary_key=True),
                Column('goods', String(50), nullable=False),
            )

    metadata.create_all(engine)
    return metadata


# Заполнение таблиц данными
# Добавляет новую строчку в таблицу Visit и Country	
def insert_country_and_visit(metadata, id_country,  name_country, id_visit, date, ip):
    table_country = metadata.tables['Country']
    table_visit = metadata.tables['Visit']
    con = metadata.bind.connect()
    # Проверка на существование в таблице Country
    temp = db.select([table_country]).where(table_country.columns.name_country == name_country)
    result = con.execute(temp).fetchone()
    if result == None:
        query = db.insert(table_country).values(c_id=id_country, name_country=name_country)
        con.execute(query)
        query = db.insert(table_visit).values(id_v=id_visit, date=date, ip=ip, contry_id=id_country)
        con.execute(query)
    else:
        id_can = result[0]    
        query = db.insert(table_visit).values(id_v=id_visit, date=date, ip=ip, contry_id=id_can)
        con.execute(query)
    

# Добавляет новую строчку в таблицу Pay
def insert_pay(metadata, id_p, num_pay, time, visit):
    table_pay = metadata.tables['Pay']
    con = metadata.bind.connect()
    query = db.insert(table_pay).values(id_p=id_p, number_pay=num_pay, time=time, visit=visit)
    con.execute(query)


# Добавляет новую строчку в таблицу Cart
def insert_cart(metadata, id_cart, time, amount, cart_num, visit, cat_goods):
    table_cart = metadata.tables['Cart']
    con = metadata.bind.connect()
    query = db.insert(table_cart).values(id_c=id_cart, time=time, amount=amount, visit=visit, cart_number=cart_num, cat_goods=cat_goods)
    con.execute(query)


# Добавляет новую строчку в таблицу History, Goods
def insert_history_goods(metadata, id_his, goods_id, time, visit, name_goods, name_category):
    table_history = metadata.tables['History']
    table_goods = metadata.tables['Goods']
    con = metadata.bind.connect()
    # Проверка на существование в таблице Goods
    temp = db.select([table_goods]).where(table_goods.columns.goods == name_goods)
    result = con.execute(temp).fetchone()
    if result == None:
        query = db.insert(table_goods).values(id_g=goods_id, goods=name_goods)
        con.execute(query)
        query = db.insert(table_history).values(id_h=id_his, id_goods=goods_id, category=name_category, time=time, visit=visit)
        con.execute(query)
    else:
        id_go = result[0]    
        query = db.insert(table_history).values(id_h=id_his, id_goods=id_go, category=name_category, time=time, visit=visit)
        con.execute(query)

