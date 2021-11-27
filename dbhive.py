import random
import datetime
import datetime as dt
import random
import time
from datetime import datetime, timedelta
import sqlite3
import timeago, datetime
from beem import Hive
from beem.account import Account
from beem.exceptions import AccountDoesNotExistsException
from pprint import pprint

from beem.instance import set_shared_blockchain_instance
from configparser import ConfigParser 
from botbase import getRewards

from beem.nodelist import NodeList
import pymssql





hived_nodes = [
  
  'https://anyx.io',
  'https://api.hive.blog',
  'https://api.openhive.network',
]




hive = Hive(node=hived_nodes)



#Read config.ini file
config_object = ConfigParser()
config_object.read("config.ini")

#Get user
userinfo = config_object["USERINFO"]

userpay = userinfo["userpay"] 

usermain = userinfo["useradm"] 


# Set Hive as shared instance
set_shared_blockchain_instance(hive)


# Conectar base de dato 


DB = sqlite3.connect("hivedbaliento.db",
                     detect_types=sqlite3.PARSE_DECLTYPES)

CURSOR = DB.cursor()



def get_maxday():

    timestop = (datetime.now()  - timedelta(days = 4.5)) # The time used is 4.5 days, you can modify it.

    return timestop



def get_ajuste():

    timestop = (datetime.now()  - timedelta(days = 0.5)) # The time used is 4.5 days, you can modify it.

    return timestop



def get_max_ops():
    """get the max count of the virtual operations from the blockchain
    Arguments:
        none
    Returns:    -1 for failure or no new virtual operations
                max_op_count in case of new virtual operations
    """
    try:
        acc = Account(usermain)
    except TypeError as err:
        print(err)
        return -1
    except AccountDoesNotExistsException:
        print("account does not exist")
        return-1
    CURSOR.execute("SELECT maxop FROM config")
    config_maxop = CURSOR.fetchone()
    max_op_count = acc.virtual_op_count()
    if max_op_count > config_maxop[0]:
        return max_op_count

    return -1

def get_delegations():
    """proporcionar información sobre la delegación a una cuenta hive
    y actualice la tabla en SQLITE que contiene los datos actuales
    Arguments:
        none
    Returns:    -1 for failure
                 0 for success
    """
    name_account = usermain
    try:
        acc = Account(usermain)
    except TypeError as err:
        print(err)
        return -1
    except AccountDoesNotExistsException:
        print("account does not exist")
        return-1

    CURSOR.execute("SELECT maxop FROM config")
    config_maxop = CURSOR.fetchone()
    max_op_count = acc.virtual_op_count()
    print("Las delegaciones que tiene %s" % name_account)


    print("Nuevas transacciones encontradas -- "
          "Intervalo de %s a %s" %(config_maxop[0], max_op_count))


    def hive_sql():
        username = 'aliento'
        conn = pymssql.connect(server='vip.hivesql.io', user='User' , password= 'Key', database='DBHive')
        cursor = conn.cursor()
        cursor.execute('select delegator, (vesting_shares), (timestamp) from  TxDelegateVestingShares  where delegatee =  %s Order by timestamp desc ' , (username, ))
        result = cursor.fetchall()
        conn.close()
        return result


    hp = hive_sql()

    for row in hp:
        delegator = row[0]
        vests = row[1]

        vests = hive.vests_to_hp(vests)

        vests = round(float(vests))

        hive_power = vests

        timestamp = row[2]
        

        CURSOR.execute("SELECT * FROM delegations WHERE delegator = ?", (delegator,))
        result = CURSOR.fetchone()


        if result is not None and timestamp > result[3]:

            CURSOR.execute("UPDATE delegations SET hivepower = ?,"
                                  " datum = ? WHERE delegator = ?", (hive_power,
                                                                          timestamp, delegator,))

            DB.commit()
            print("update")
            print("de: %s Vests: %s HP: %s Datum: %s " % (delegator, vests,
                                                                       hive_power, timestamp))
        elif result is None:
            print("insert")
            print("de: %s Vests: %s HP: %s Datum: %s " % (delegator, vests,hive_power,
                                                                timestamp))
            CURSOR.execute("INSERT INTO delegations (delegator, hivepower, datum)"
                                " VALUES (?,?,?)", (delegator, hive_power, timestamp))

            DB.commit()
          



    CURSOR.execute("DELETE FROM delegations WHERE hivepower==0")
    DB.commit()
    return 0






def get_start():

	CURSOR.execute("SELECT dateops FROM fecha")
	config_date = CURSOR.fetchone()
	timestart = config_date[0]
	timestart = datetime.datetime.fromisoformat(timestart)
	timestart = (timestart - datetime.timedelta(days=14)) # The time used is 4.5 days, you can modify it.
	return timestart



def get_delepay():
    """proporcionar información sobre la delegación a una cuenta hive
    y actualice la tabla en SQLITE que contiene los datos actuales
    Arguments:
        none
    Returns:    -1 for failure
                 0 for success
    """
    name_account = usermain
    try:
        acc = Account(usermain)
    except TypeError as err:
        print(err)
        return -1
    except AccountDoesNotExistsException:
        print("account does not exist")
        return-1

    CURSOR.execute("SELECT dateops FROM fecha")
    config_date = CURSOR.fetchone()

    timestop = config_date[0]

    
    date_st = get_maxday()

    print("Los usuarios aptos para pago que tiene %s" % name_account)

    print("Nuevas transacciones encontradas -- "
          "Intervalo de %s a %s" %(date_st , timestop))
   # c.execute("SELECT * FROM delegations")
   # delegations = c.fetchall()
    #print (delegations)
    def hive_sql():
        username = 'aliento'
        conn = pymssql.connect(server='vip.hivesql.io', user='user' , password= 'key', database='DBHive')
        cursor = conn.cursor()
        cursor.execute('select delegator, (vesting_shares), (timestamp) from  TxDelegateVestingShares  where delegatee =  %s Order by timestamp desc ' , (username, ))
        result = cursor.fetchall()
        conn.close()
        return result


    hp = hive_sql()

    for row in hp:
        delegator = row[0]
        vests = row[1]

        vests = hive.vests_to_hp(vests)

        vests = round(float(vests))

        hive_power = vests

        timestamp = row[2]

        if timestamp < date_st:



            CURSOR.execute("SELECT * FROM payment WHERE delegator = ?", (delegator,))
            result = CURSOR.fetchone()
            #print (result)
            if result is not None and timestamp > result[3]:
                CURSOR.execute("UPDATE payment SET hivepower = ?,"
                               " datum = ? WHERE delegator = ?", (hive_power,
                                                                  timestamp, delegator,))
                DB.commit()
                print("update")
                print("de: %s Vests: %s HP: %s Datum: %s " % (delegator, vests ,
                                                               hive_power, timestamp))
            elif result is None:
                print("insert")
                print("de: %s Vests: %s HP: %s Datum: %s " % (delegator, vests ,hive_power,
                                                               timestamp))
                CURSOR.execute("INSERT INTO payment (delegator, hivepower, datum)"
                               " VALUES (?,?,?)", (delegator, hive_power, timestamp))
                DB.commit()
    CURSOR.execute("DELETE FROM payment WHERE hivepower==0")
    DB.commit()
    return 0


def calculate_percentage():
    """calcula el porcentaje para cada delegador en comparación con el
    delegación total

    """
    CURSOR.execute("SELECT Sum(hivepower) FROM payment ")
    result = CURSOR.fetchone()
    gesamt_hp = result[0]
    
    CURSOR.execute("SELECT * FROM payment")
                    
    rows = CURSOR.fetchall()
    for row in rows:
        percentage = (row[2] / gesamt_hp) * 100
        percentage = round(percentage, 2)
        CURSOR.execute("UPDATE payment SET porcentaje = ? WHERE delegator = ?"
                       , (percentage, row[1],))
        DB.commit()

        print("User %s Posee %s Porcentaje del total de delegaciones" % (row[1], percentage))


def calculate_days():
    """calcula el porcentaje para cada delegador en comparación con el
    delegación total
    Arguments:
        none
    Returns:
        none
    """

    CURSOR.execute("SELECT * FROM delegations")
                    
    rows = CURSOR.fetchall()
    for row in rows:
        date = row[3]

        timea = date
        #datenow = datetime.datetime.today()
        #now = datenow - timeago 
        timea= timeago.format(timea)

        CURSOR.execute("UPDATE delegations SET days = ? WHERE delegator = ?"
                       , (timea, row[1],))
        DB.commit()

        print("User %s Delego hace %s" % (row[1], timea))



 


def calculate_pay():
    """calcula el porcentaje para cada delegador en comparación con el
    delegación total

    Define tu criterio

    """
    CURSOR.execute("SELECT porcentaje, hivepower, delegator FROM payment WHERE  hivepower >= 50 and  porcentaje > 0")
    
   
    rows = CURSOR.fetchall()
    hivetotal = getRewards() * 0.85
    for row in rows:
        
        porcen = (row[0])
        pay = (porcen * (hivetotal)) / 100 
        pay = round(pay, 2)
        CURSOR.execute("UPDATE payment SET pay = ? WHERE delegator = ?"
                       , (pay, row[2],))
        DB.commit()
        
        print("Calculado pago User %s hat %s  y monto a pagar %s " % (row[2], porcen, pay))
    
    return 0     



def suma_pay():

    ### Puedes agregar todos lo que quieres excluir

    CURSOR.execute("SELECT sum(pay), delegator FROM payment  WHERE hivepower >= 50 and delegator NOT IN ('theycallmedan', 'eddiespino', 'grisvisa')") 




    result  = CURSOR.fetchone()
    suma = result[0]
    print ( suma )
    return suma











def refres():

    X = get_delegations()
        
    Y = get_delepay()
    print ("pagos listos")
    Z = calculate_percentage()

    paycalculo =  calculate_pay()

    ago = calculate_days()

    print ("listo refres")

    CURSOR.execute("SELECT maxop FROM config")
    config_maxop = CURSOR.fetchone()

    acc = Account(usermain)
    max_op_count = acc.virtual_op_count()
    max_date_count = get_ajuste()
    CURSOR.execute("UPDATE config SET maxop = ?", (max_op_count,))
    CURSOR.execute("UPDATE fecha SET dateops = ?", (max_date_count,))

    DB.commit()



def query():
    print ("actualizando tabla")
    SWITCH = get_max_ops()
    if SWITCH > 0:
        X = refres()

        return 0
        

    if SWITCH < 0:
        print("Sin transacciones nuevas, cálculo de las nuevas"
              " Por tanto, no es necesaria la distribución. Saltar")

        return -1

def querypay():
    print ("actualizando tabla de payment")
    SWITCH = get_max_ops()



def suma():
    suma = suma_pay()
    
    return suma





def formatdate():

    CURSOR.execute("SELECT dateops FROM fecha")

    config_date = CURSOR.fetchone()
    acc = Account(usermain, blockchain_instance=hive)
    max_date_count = (acc['created'])
    max_date_count = (max_date_count)
    CURSOR.execute("UPDATE fecha SET dateops = ?", (max_date_count,))
    CURSOR.execute("UPDATE config SET maxop = ?", (1,))
    CURSOR.execute("DELETE FROM payment ")
    CURSOR.execute("DELETE FROM delegations ")
    print (max_date_count )
    DB.commit()

    return -1


