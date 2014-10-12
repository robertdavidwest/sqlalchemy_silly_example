import pandas
import sqlalchemy
from sqlalchemy import create_engine, MetaData, VARCHAR, TEXT, Integer, Table, Column, ForeignKey

####### Dead parrot sketch dataset
#######
#######

#######################################################################################################################
#######################################################################################################################
### Initial Dataset ###
monty_python_boys = ['Graham Chapman', 'Eric Idle', 'Terry Gilliam', 'Terry Jones', 'John Cleese', 'Michael Palin']
featured_in_dead_parrot = [1,0,0,1,1,1]
parrot_df = pandas.DataFrame({'Actor Name': monty_python_boys, 'Featured in dead parrot sketch' : featured_in_dead_parrot})
parrot_df.index.name = 'actor_id' # label index

# 2 new variables to be added to the dataset
char_name = ['no-nonsense colonel',None,None,'Station attendant/Brain Surgeon','Mr Praline/Customer','Original Shopkeeper', 'Shopkeeper in Bolton/Ipswitch']
num_chars_played = [1,0,0,1,1,2,2]

# Add character to dataset
monty_python_boys.append('Michael Palin')
featured_in_dead_parrot.append(1)
parrot_df2 = pandas.DataFrame({'Actor Name': monty_python_boys, 'Featured in dead parrot sketch' : featured_in_dead_parrot, 'num characters played in sketch' : num_chars_played, 'Character' : char_name})

# Creare alternative, relational tables for MySQL DB
monty_python_boys = ['Graham Chapman', 'Eric Idle', 'Terry Gilliam', 'Terry Jones', 'John Cleese', 'Michael Palin']
featured_in_dead_parrot = [1,0,0,1,1,1]
num_chars_played = [1,0,0,1,1,2]

parrot_actor_df = pandas.DataFrame({'Actor Name': monty_python_boys, 'Featured in dead parrot sketch' : featured_in_dead_parrot,'num characters played in sketch' : num_chars_played})
parrot_actor_df.index.name = 'actor_id' # label index

monty_python_boys = ['Graham Chapman', 'Terry Jones', 'John Cleese', 'Michael Palin','Michael Palin']
char_name = ['no-nonsense colonel','Station attendant/Brain Surgeon','Mr Praline/Customer','Original Shopkeeper', 'Shopkeeper in Bolton/Ipswitch']
parrot_char_df = pandas.DataFrame({'actor_id': [0, 3, 4, 5, 5], 'Character' : char_name})

#######################################################################################################################
#######################################################################################################################
#### Create DB Strucuture using SQLAlchemy

f = open('mysql_pword.txt','r')
pword = f.read()
f.close()
engine = create_engine("mysql+mysqldb://root:"+pword+"@localhost/parrot_db")

## Drop sql tables if they already exist
meta = MetaData(bind=engine)

characters = Table('characters',meta)
characters.drop(engine,checkfirst=True)

actors = Table('actors',meta)
actors.drop(engine,checkfirst=True)

########################################
### SET META ENVIRONMENT FOR DATABASE

meta = MetaData(bind=engine)


## ACTOR PARROT TABLE ###
table_actors = Table('actors', meta,
	Column('actor_id', Integer, primary_key=True, autoincrement=False),
	Column('Actor Name', TEXT, nullable=True),
	Column('Featured in dead parrot sketch', TEXT, nullable=True),
	Column('num characters played in sketch', TEXT, nullable=True)
)

## CHARACTER PARROT TABLE ###
table_characters = Table('characters', meta,
	Column('Character', TEXT, nullable=True),
	Column('actor_id', Integer, ForeignKey('actors.actor_id'))
)

#######################################################################################################################
### Create all tables 

meta.create_all(engine)

#######################################################################################################################
### Send data to mySQL DATABASE
parrot_actor_df.to_sql('actors',engine,flavor='mysql', if_exists='append',index=True)
parrot_char_df.to_sql('characters',engine,flavor='mysql', if_exists='append',index=False)
