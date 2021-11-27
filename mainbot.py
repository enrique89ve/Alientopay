import discord
import datetime
import time  
import sqlite3
from sqlite3 import Error
import re
import sys
import os
import json
from discord.ext import commands
from discord.ext.commands import Bot
from discord.ext import commands
from discord.ext.commands.errors import CommandNotFound
import datetime
from botbase import (
    getapr,
    gettotalhp,
    getRewards,
    getRw,
     
)

from beem import Hive
from beem.account import Account
from beem.exceptions import AccountDoesNotExistsException
from pprint import pprint

from beem.instance import set_shared_blockchain_instance
from configparser import ConfigParser 
from botbase import getRewards

from dbhive import (query, suma, get_delepay, querypay, calculate_days, formatdate) 


import os
import pytz 



hived_nodes = [

  'https://anyx.io',
  'https://api.hive.blog',
  'https://api.openhive.network',
]


#Read config.ini file
config_object = ConfigParser()
config_object.read("config.ini")

#Get user
userinfo = config_object["USERINFO"]

userpay = userinfo["userpay"] 

usermain = userinfo["useradm"] 

wif = userinfo["active"] 

hive = Hive(node=hived_nodes,
	nobroadcast=False, # Set to false when want to go live
	keys=[wif]
)
# Set testnet as shared instance
set_shared_blockchain_instance(hive)





DB = sqlite3.connect("hivedbaliento.db",
                     detect_types=sqlite3.PARSE_DECLTYPES)

CURSOR = DB.cursor()






bot = commands.Bot(command_prefix='!')
# Print When Ready
@bot.event
async def on_ready():
  print("Ready!")


def fecha():

	datenow = datetime.datetime.today()
	date = (datenow.astimezone(pytz.timezone("America/Cancun"))).strftime("%b %d %Y %H:%M:%S") 
	return date 


print (fecha())

@bot.command(pass_context=True)
async def stats(ctx):
	await ctx.send("Espera mientras el bot conecta con la blockchain Hive")
	
	curation30 = getRw()
	curation = getRewards()
	pago = getRewards() * 0.85
	pago = round(pago, 2)
	apr = round(getapr(),2)
	hp = gettotalhp()
	fechaa = fecha()
	embed=discord.Embed(title="Estadisticas de la cuenta Aliento en HIVE")
	embed.add_field(name="Curación 7 días", value=round(curation,2), inline=True)
	embed.add_field(name="Curación 30 días", value=round(curation30,2), inline=True)
	embed.add_field(name="HP Total", value=round(hp,2), inline=True)
	embed.add_field(name="APR %", value= (apr), inline=True)
	embed.add_field(name="El 85% Utilizado para el pago", value= (pago), inline=True)


	embed.add_field(name="FECHA", value= fechaa , inline=True)

	embed.set_footer(text="Estadisticas extraidas de la blockchain Hive")
	await ctx.channel.send(embed=embed)    


@bot.command(pass_context=True)
async def test(ctx):
	author = ctx.message.author
	await ctx.channel.send(author)
	print ("listo comando test")


# Extrae los delegadores y la cantidad a atransferir a la cuenta 





def repde():

	column_title_1 = 'Delegador'
	column_title_2= 'Hp delegado'
	column_title_3= 'Fecha de delegación'
	column_title_4= 'Dias'

	main_report = (
                  '|' + 
                  column_title_1 + ' | ' +
                  column_title_2 + ' | ' +
                  column_title_3 + ' | ' +
                  column_title_4 + ' |\n'+
                  '| --- | --- | --- | --- | --- |\n'
                  )
	c_list = {}
	finished = False	


	DB = sqlite3.connect("hivedbaliento.db",
	                     detect_types=sqlite3.PARSE_DECLTYPES)

	CURSOR = DB.cursor()	

	for row in CURSOR.execute("SELECT delegator, hivepower, datum, days from delegations WHERE hivepower > 0  ORDER BY hivepower DESC" ):


		delegador = row[0]
		hp = row[1]
		#porcen = row[2]
		day = row[3]
		fecha =  row[2].strftime("%b %d %Y ")

		poll = ("| @%s | %s |  %s | %s |" % (delegador, hp, fecha, day ))
		main_report = 		(main_report + 
							poll + 
							'\n'	
							)



	report = (main_report)

	return report



		#await ctx.channel.send("Calculado pago User %s hat %s  y monto a pagar %s " % (delegador, porcen, pago, fecha))
	CURSOR.close()
		
# Buscar delegadores normal
@bot.command(pass_context=True)
async def db(ctx):




	await ctx.send(f"Buscando a todos los delegadores de Aliento")


	actu = query()


	time.sleep(2)
	
	extraer = repde()

	time.sleep(1)

	file = open("delegadoresdeAliento.txt", "w")
	file.write(extraer)
	
	file.close() 
	await ctx.channel.send(file = discord.File( fp = "delegadoresdeAliento.txt"))




def tablaparapagos():

	column_title_1 = 'Delegador'
	column_title_2= 'Hp delegado'
	column_title_3= '% De participación'
	column_title_4 = 'Cantidad de pago ($HIVE)'
	column_title_5 = 'Fecha de delegación'

	main_report = (
                  '|' + 
                  column_title_1 + ' | ' +
                  column_title_2 + ' | ' +
                  column_title_3 + ' | ' + 
                  column_title_4 + ' | ' + 
                  column_title_5 + ' |\n'+
                  '| --- | --- | --- | --- | --- |\n'
                  )
	c_list = {}
	finished = False	


	DB = sqlite3.connect("hivedbaliento.db",
	                     detect_types=sqlite3.PARSE_DECLTYPES)

	CURSOR = DB.cursor()	

	for row in CURSOR.execute("SELECT delegator, hivepower, porcentaje, datum, pay from payment WHERE hivepower >= 50  ORDER BY porcentaje DESC" ):


		resta =  datetime.datetime.utcnow()  - row[3] 
		if resta > datetime.timedelta(days=1):
			delegador = row[0]
			hp = row[1]
			porcen = row[2]
			pago = row[4]
			fecha =  row[3].strftime("%b %d %Y ")

			poll = ("| @%s | %s |  %s | %s |  %s | " % (delegador, hp, porcen, pago, fecha ))
			main_report = 		(main_report + 
								poll + 
								'\n'	
							)



	report = (main_report)

	return report



		#await ctx.channel.send("Calculado pago User %s hat %s  y monto a pagar %s " % (delegador, porcen, pago, fecha))
	CURSOR.close()	




@bot.command(pass_context=True)
async def dbpay(ctx):

	await ctx.send("Buscando a todos los delegadores de Aliento y su pagos respectivos")
	
	actu = query()

	

	

	# actualizar

	

	await ctx.send("Recopilando datos")




	totalpago = suma()

	totalpago = round(totalpago, 2)

	
	
	extraer = tablaparapagos()

	time.sleep(3)

	# añadir a la base datos

	file = open("delegadorespagos.txt", "w")
	file.write(extraer)
	
	file.close() 

	await ctx.channel.send(file = discord.File( fp = "delegadorespagos.txt"))


	mensaj = ("La cantidad de Hive a Pagar es:  %s  " % (totalpago))

	await ctx.channel.send(mensaj)


@bot.command()
async def act(ctx):

	CURSOR.execute("SELECT dateops FROM fecha")
	config_date = CURSOR.fetchone()

	await ctx.send("La base datos  de pagos se actualiza a partir de la fecha %s" % (config_date[0]))
	actu = query()
	time.sleep(3)

	await ctx.send('Actualizada con éxito')


@bot.command()
async def day(ctx):
    await ctx.send('La base de datos se actualizará')

    

    actu = calculate_days() 

    time.sleep(3)

    await ctx.send('Actualizada con éxito')

@commands.has_role("AP")
@bot.command()
async def pay(ctx):


	channels = ["bot", "test", "aliento-pay-bot"]

	if str(ctx.channel) in channels: 

	    await ctx.send('Transfiere los Hives a la cuenta de Aliento.pay para proceder con el pago')


	    while True:
	    	acc = Account(userpay)
	    	to_distribute = acc.get_balance("available", "HIVE")


	    	if to_distribute < 1:
	    		print ("Esperando pago...")
	    		time.sleep(5)
	    	else:
	    		print ("Hive recibidos con !éxito! ")
	    		await ctx.send("La cuenta de Aliento.pay, tiene la candidad de %s" % (to_distribute))
	    		break	

	    await ctx.send('Procediendo con el pago de los %s , espere mientras finaliza el proceso' % suma())

	    time.sleep(3)


	    acc = Account(userpay, blockchain_instance=hive)     

	    CURSOR.execute("SELECT delegator, datum, porcentaje, hivepower, pay from payment WHERE pay > 0  ORDER BY porcentaje" )
	    result = CURSOR.fetchall()
	    



	    for row in result:

	    	datum = row[1]
	    	#hive = to_distribute * float(row[2]) / 100
	    	#hive = round(hive, 2)
	    	hp = row[3]
	    	pay = row[4]
	    	#hive = hive.replace(" HIVE", "")
	    	delegator = row[0]

	    	

	    	if hp >= 50 and delegator!='grisvisa' and delegator!='eddiespino' and delegator!='theycallmedan':
	    		memo = "Thank you @%s  for your support to the Aliento Project! These are your weekly earnings. " % (delegator)
	    		time.sleep(2)
	    		pprint(acc.transfer(delegator, pay, "HIVE", memo=memo ))
	    		print("Delegator %s , Porcentaje %s , cantidad de HIVE %s " % (delegator, row[2], pay))
	    		guild = ctx.guild
	    		channel = guild.get_channel(909998329746817044)
	    		await channel.send("Listo el pago de %s por la cantidad de %s HIVE" % (delegator, pay))
	    		time.sleep(3)
	    await ctx.send("Pago realizado con exito")		
	    


@bot.command()
async def ping(ctx):

	
	await ctx.send('pong')
	await ctx.message.delete()



@bot.command()
async def arepa(ctx):

	
	await ctx.send('Nos gusta la arepa')
	await ctx.message.delete()


@bot.command()
async def setpay(ctx, *, content: float):

	hivetotal =  content
	await ctx.send("Recalculando pago con el pago %s HIVE" % (hivetotal))
	CURSOR.execute("SELECT porcentaje, hivepower, delegator FROM payment WHERE  hivepower >= 50 and  porcentaje > 0")
	rows = CURSOR.fetchall()
	for row in rows:
		porcen = (row[0])
		pay = (porcen * (hivetotal)) / 100
		pay = round(pay, 2)
		CURSOR.execute("UPDATE payment SET pay = ? WHERE delegator = ?"
                       , (pay, row[2],))
		DB.commit()
		print("Calculado pago User %s hat %s  y monto a pagar %s " % (row[2], porcen, pay))

		
	await ctx.send('Tabla de pagos actualizada con éxito')
	extraer = tablaparapagos()
	file = open("delegadorespagos.txt", "w")
	file.write(extraer)
	
	file.close() 

	await ctx.channel.send(file = discord.File( fp = "delegadorespagos.txt"))     




bot.remove_command('help')
@bot.command()
async def help(ctx):

	embed=discord.Embed(title="Comandos del BOT para Aliento Pay")
	embed.add_field(name=".help", value= "Solicita ayuda", inline=True)
	embed.add_field(name=".set", value="reinicia la fecha de escaneo del bot", inline=True)
	embed.add_field(name=".dbpay", value="Tabla con los usuarios que delegan al proyecto y recibiran pagos", inline=True)
	embed.add_field(name=".setpay", value="Actualiza la tabla de pagos con una cantidad de Hive solicitada, ejemplo: .setpay 250", inline=True)
	embed.add_field(name=".db", value=r" Tabla general con todos los usuarios que delegan a Aliento", inline=True)
	embed.add_field(name=".stats", value= "Solicitar todos los datos básicos de aliento", inline=True)
	embed.add_field(name=".pay", value= "Ejecutar el pago", inline=True)
	embed.add_field(name=".act", value= "Actualizar la tabla", inline=True)

	embed.add_field(name=".dist", value= "Distribuir pago de prueba", inline=True)
	embed.set_footer(text="Aliento Pay")


	await ctx.channel.send(embed=embed)


@bot.command()
async def set(ctx):
	run = formatdate()
	CURSOR.execute("SELECT dateops FROM fecha")
	config_date = CURSOR.fetchone()

	date = config_date[0]


	await ctx.send('El bot se ha configurado para iniciar en la fecha %s ' % (date))



bot.run("OTA5OTczODM5MjY0NDkzNTc4.YZMFLQ.bazMYlrmVzucr5Op1w3G8rBbpz0") # Replace Token 