from logging import root
from tkinter import*
from tkinter import messagebox
from tkinter import ttk
import pandas as pd
import psycopg2 as pg 
import os
from tkinter.messagebox import showinfo
from tkinter.filedialog import askopenfilename
from json.tool import main
import os
import subprocess
from time import time
import mysql.connector
from sqlalchemy import create_engine

#Criar Janela

jan = Tk()
jan.title("Medusa")
jan.geometry("800x400")
jan.configure(background="white")
jan.resizable(width=False, height=False) #não é possivel alterar o tamanho da janela 
#jan.attributes("-alpha",0.9) #transparencia na janela
#Carregando Imagens


#Widgets
LeftFrame = Frame(jan, width=200, height=400, bg="BLACK", relief="raise") #Lado esquerdo da janela
LeftFrame.pack(side=LEFT)

RightFrame = Frame(jan, width=595, height=400, bg="BLACK" , relief="raise")#Lado direito da janela
RightFrame.pack(side=RIGHT)


#SELECIONAR O SISTEMA QUE SERÁ EXTRAIDO

combobox = Label(jan,
                    text = "Escolha o sistema",font=("Century Gothic",10), bg="BLACK", fg="white")

combobox.place(x=40, y=25)

combobox = ttk.Combobox(jan, values=["Big","Sistema MDB"])

combobox.place(x=10, y=50)
combobox.current(0)

def InserirSistema():
     
    if combobox.get() == 'Big':

        Tk().withdraw() # Isto torna oculto a janela principal

        filename = askopenfilename() # Isto te permite selecionar um arquivo
        
        BaseMYSQL = Label(RightFrame, text="Nome da base MYSQL:", 
                             font=("Century Gothic", 10), 
                             bg="BLACK", 
                             fg="white")
        BaseMYSQL.place(x=27, y=90)        
        
        BaseMYSQLEntry = ttk.Entry(RightFrame, width=25)
        BaseMYSQLEntry.place(x=27, y=110)

        # CAMINHO DUMP MYSQL


        CaminhoDumpMySQL = Label(RightFrame, text="Caminho do Dump:", 
                             font=("Century Gothic", 10), 
                             bg="BLACK", 
                             fg="white")
        CaminhoDumpMySQL.place(x=27, y=130)        
        
        CaminhoDumpMySQLEntry = ttk.Entry(RightFrame, width=25)
        CaminhoDumpMySQLEntry.place(x=27, y=150)
        CaminhoDumpMySQLEntry.insert(0,filename)

        # LABEL E ESCRITA DO NOME DO BANCO IMP
        
        BaseImportacao = Label(RightFrame, text="Nome da Base Imp:", 
                             font=("Century Gothic", 10), 
                             bg="BLACK", 
                             fg="white")
        BaseImportacao.place(x=27, y=185)

        BaseImportacaoEntry = ttk.Entry(RightFrame, width=25)
        BaseImportacaoEntry.place(x=27, y=210)

        # LABEL E ESCRITA DO SERVER MYSQL
    
        IpMYSQL = Label(RightFrame, text="IP MYSQL:", 
                             font=("Century Gothic", 10), 
                             bg="BLACK", 
                             fg="white")
        IpMYSQL.place(x=27, y=240)

        IpMYSQLEntry = ttk.Entry(RightFrame, width=25)
        IpMYSQLEntry.place(x=27, y=270)
        
        def BigGeracao():
            engineMysql = create_engine(f"mysql+pymysql://root:supertux@{IpMYSQLEntry.get()}/mysql")
            
            conMYSQL = mysql.connector.connect(user='root',password='supertux',host='%s'%(IpMYSQLEntry.get()), database='mysql')

            cursorMYSQL = conMYSQL.cursor()
            conMYSQL.autocommit = True


            CriacaoBancoMysql = 'CREATE DATABASE %s;'%(BaseMYSQLEntry.get()) 
            
            DuplicidadeBANCO = f'''DROP DATABASE IF EXISTS {BaseMYSQLEntry.get()};'''

            cursorMYSQL.execute(DuplicidadeBANCO)

            cursorMYSQL.execute(CriacaoBancoMysql)

            print('Criado a base mysql')
            
            subprocess.run(f'iconv -f latin1 -t UTF-8 -o /home/alpha7/apps_python/DUMP_BIG/dump.utf8.sql {filename}',shell=True, check=True)
            
            subprocess.run('''
            sed -e 's/TYPE=MyISAM/ENGINE=MyISAM/g' /home/alpha7/apps_python/DUMP_BIG/dump.utf8.sql > /home/alpha7/apps_python/DUMP_BIG/dump.utf8.engine.sql''',
            shell=True, check=True)
            
            print('Base convertida para UTF-8')

            print('INICIANDO O RESTORE NO MYSQL')

            subprocess.run(f'mysql -h{IpMYSQLEntry.get()} -uroot -psupertux -f -D {BaseMYSQLEntry.get()}< /home/alpha7/apps_python/DUMP_BIG/dump.utf8.engine.sql'
,                           shell=True, check=False)



            print('Banco restaurado no MYSQL')

            conn = pg.connect(   
   database="postgres", user='postgres', password='supertux', host='localhost', port= '5433'
)
            cursor = conn.cursor()
            conn.autocommit = True

            sql = f'''DROP DATABASE IF EXISTS {BaseImportacaoEntry.get()};'''
            sql2 = f'''CREATE DATABASE {BaseImportacaoEntry.get()}
            WITH
            TEMPLATE = clean_chinchila_imp; '''

            cursor.execute(sql)
            cursor.execute(sql2)
            conn.close()

            print(f"BANCO: {BaseImportacaoEntry.get()} CRIADA COM SUCESSO")

            engine = create_engine('postgresql://postgres:supertux@localhost:5433/{}'.format(BaseImportacaoEntry.get()))

            engineMysql = create_engine(f"mysql+pymysql://root:supertux@{IpMYSQLEntry.get()}/{BaseMYSQLEntry.get()}")

            ListarTabelas = pd.read_sql_query(f'''SHOW TABLES from {BaseMYSQLEntry.get()} where Tables_in_{BaseMYSQLEntry.get()} not in ('movment','receber') ''', engineMysql)

            for x in ListarTabelas[f'Tables_in_{BaseMYSQLEntry.get()}']:
                AllInfosTables = pd.read_sql_query('select * from %s'%(x), engineMysql)
                print(x + ' Gerando CSV')
                AllInfosTables.columns = AllInfosTables.columns.str.lower().str.replace(' ', '_')     
                AllInfosTables.to_csv('CSV_BIG/%s'%(x),sep=';',index=False, encoding='utf-8', chunksize=1000)
                InserirDadosTabela = pd.read_csv('CSV_BIG/%s'%(x),delimiter=';',index_col=0,low_memory=False ).to_sql('%s'%(x),if_exists='replace',con=engine)
                print(x + ' Concluído')
            
            print('GERAÇÃO DOS DADOS CONCLUÍDOS')


        GeracaoButton = ttk.Button(RightFrame, text="Iniciar Geração", width=15, command=BigGeracao)
        GeracaoButton.place(x=450, y=340)


    elif combobox.get() == 'Sistema MDB':
        
        Tk().withdraw() # Isto torna oculto a janela principal
        filename = askopenfilename() # Isto te permite selecionar um arquivo

        BaseImportacao = Label(RightFrame, text="Nome da Base Imp:", 
                             font=("Century Gothic", 10), 
                             bg="BLACK", 
                             fg="white")
        BaseImportacao.place(x=27, y=185)

        BaseImportacaoEntry = ttk.Entry(RightFrame, width=25)
        BaseImportacaoEntry.place(x=27, y=210)

        # CAMINHO MDB


        CaminhoMDB = Label(RightFrame, text="Caminho do MDB:", 
                             font=("Century Gothic", 10), 
                             bg="BLACK", 
                             fg="white")
        CaminhoMDB.place(x=27, y=130)        
        
        CaminhoMDBEntry = ttk.Entry(RightFrame, width=25)
        CaminhoMDBEntry.place(x=27, y=150)
        CaminhoMDBEntry.insert(0,filename)

        def MDBGeracao():

            conn = pg.connect(   
            database="postgres", user='postgres', password='supertux', host='localhost', port= '5433'
            )
            cursor = conn.cursor()
            conn.autocommit = True

            sql = f'''DROP DATABASE IF EXISTS {BaseImportacaoEntry.get()};'''
            sql2 = f'''CREATE DATABASE {BaseImportacaoEntry.get()}
            WITH
            TEMPLATE = clean_chinchila_imp; '''
            cursor.execute(sql)
            cursor.execute(sql2)
            conn.close()

            print(f"BANCO: {BaseImportacaoEntry.get()} CRIADA COM SUCESSO")

            engine = create_engine('postgresql://postgres:supertux@localhost:5433/{}'.format(BaseImportacaoEntry.get()))



            subprocess.run("java -jar client-0.0.5.jar convert --output-format=csv %s ORIGINAIS/"%(filename), shell=True, check=True)
            subprocess.run("rm ORIGINAIS/*.columns", shell=True, check=True)


            pasta = '/home/alpha7/Convert_mdb/ORIGINAIS/'
            for diretorio, subpastas, arquivos in os.walk(pasta):
                for arquivo in arquivos:
                    virgula = pd.read_csv('/home/alpha7/Convert_mdb/ORIGINAIS/%s'%(arquivo), delimiter=',')
                    pontovirgula = virgula.to_csv('/home/alpha7/Convert_mdb/TRATADOS/%s'%(arquivo), sep=';',index=False)
                    pd.read_csv('/home/alpha7/Convert_mdb/TRATADOS/%s'%(arquivo), delimiter=';', index_col=0).to_sql('%s'%(arquivo.replace('.csv', '').lower().replace(' ', '_')), con=engine,if_exists='replace')
                    print(' %s CRIADA COM SUCESSO'%(arquivo.replace('.csv', '').lower().replace(' ', '_').replace('','-')))
         
        GeracaoButton = ttk.Button(RightFrame, text="Iniciar Geração", width=15, command=MDBGeracao)
        GeracaoButton.place(x=450, y=340)




#Botões

Aplicar = ttk.Button(LeftFrame, text="Aplicar", width=15,  command=InserirSistema)
Aplicar.place(x=35, y=340)


jan.mainloop()