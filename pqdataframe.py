import pandas as pd
from pandas.io import sql
from datetime import datetime
import os,mysql.connector,yaml
from sqlalchemy import create_engine

with open("config.yml", 'r') as ymlfile:
    cfg = yaml.safe_load(ymlfile)

#_,_,filenames2 = next(os.walk('../pqreport/media/run/'))
#runfiles = list(map(lambda x : x, filenames2))

folderpath = '../pqreport/media/'
csvfolder = 'csv'

if os.path.exists(folderpath+'run/runscript'):
    '''
    s_file = runfiles[0].split('_')
    print(s_file)
    if('lock' in s_file):
        exit
    else:
    '''
    os.rename(folderpath+'run/runscript',folderpath+'run/runscript_lock')
    _,_,filenames = next(os.walk(folderpath+csvfolder+'/'))
    allfiles = list(map(lambda x : x.split('_')[0].split('.')[0], filenames))
    allfiles.sort()
    print(allfiles)

    print('running')
    latestfile = allfiles[-1]
    uniquefiles = list(set(allfiles))
    uniquefiles.sort()
    secondlastfile = uniquefiles[-2]

    latestfiles = list(filter(lambda x: x.startswith(latestfile),filenames))
    secondfiles = list(filter(lambda x: x.startswith(secondlastfile),filenames))

    latestfiles.sort()
    secondfiles.sort()

    #print(os.walk('media/csv/'))
    path1 = folderpath+csvfolder+'/'+str(latestfiles[-1])
    path2 = folderpath+csvfolder+'/'+str(secondfiles[-1])
    df1=pd.read_excel(path1).sort_values(by=['ITEM_NO','PRODTYPE_TEXT','PART_TEXT','MATERIAL_TEXT'], ascending=False)
    df2=pd.read_excel(path2).sort_values(by=['ITEM_NO','PRODTYPE_TEXT','PART_TEXT','MATERIAL_TEXT'], ascending=False)


    df1['material'] = df1['PRODTYPE_TEXT'].astype(str)+' '+df1['PART_TEXT'].astype(str)+' '+df1['MATERIAL_TEXT'].astype(str)
    df2['material'] = df2['PRODTYPE_TEXT'].astype(str)+' '+df2['PART_TEXT'].astype(str)+' '+df2['MATERIAL_TEXT'].astype(str)

    #df1['sortedcom'] = df1['PRODTYPE_TEXT'].astype(str)+' '+df1['PART_TEXT'].astype(str)+' '+df1['MATERIAL_TEXT'].astype(str)
    #df2['sortedcom'] = df2['PRODTYPE_TEXT'].astype(str)+' '+df2['PART_TEXT'].astype(str)+' '+df2['MATERIAL_TEXT'].astype(str)
    #print(df1)
    df01 = df1.groupby('ITEM_NO').agg({
                            'ITEM_NAME':'first',
                            'HFB_NO':'first',
                            'PRA_NO':'first',
                            'PA_NO':'first',
                            'SALES_START':'first',
                            'SALES_END':'first', 
                            'material': ', '.join
                            }).sort_values('material', ascending=False).reset_index()

    df02 = df2.groupby('ITEM_NO').agg({
                            'ITEM_NAME':'first',
                            'HFB_NO':'first',
                            'PRA_NO':'first',
                            'PA_NO':'first',
                            'SALES_START':'first',
                            'SALES_END':'first', 
                            'material': ', '.join
                            }).sort_values('material', ascending=False).reset_index()

    df = df01[~(df01['ITEM_NO'].isin(df02['ITEM_NO']) & df01['material'].isin(df02['material']) )].reset_index(drop=True)
    pd.set_option('display.max_columns', None)


    df['change'] = df['ITEM_NO'].map(df02.set_index('ITEM_NO')['material']).fillna('')


    #df['id'] = df.index
    ##df['change'] = np.where( df['ITEM_NO'].isin(df02['ITEM_NO']) == True, df02.loc[df02['ITEM_NO']].['combined'].values[0], '')
    df['fullmaterial'] = df['change'].str.replace('nan','')
    df['material'] = df ['material'].str.replace('nan','')
    df["SALES_START"] = df["SALES_START"].replace('NaT',' ')
    df["SALES_END"] = df["SALES_END"].replace('NaT', ' ')

    
    now = datetime.now()    
    formatted_date = now.strftime('%Y-%m-%d')
    
    ##formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
    ##df['created_on'] = datetime.strptime(formatted_date, '%Y-%m-%d %H:%M:%S')
    ##df['sort'] = 0
    
    
    df['status']=df.apply(lambda row: 1 if row.fullmaterial !='' else 0 , axis=1)
    #print(df)

    #WORKING
    hostname=cfg['mysql']['host']
    dbname=cfg['mysql']['db']
    uname=cfg['mysql']['user']
    pwd=""
    mydb = mysql.connector.connect(
        host=cfg['mysql']['host'],
        database=cfg['mysql']['db'],
        user=cfg['mysql']['user'],
        password=''
    )
    mycursor = mydb.cursor()
    mycursor.execute(""" DELETE FROM material_articles """)
    mydb.commit()
    for _, row in df.iterrows():
        a = (row[0],formatted_date,row[9],row[2],row[7],row[1],row[4],row[3],0,row[10])
        print(a)
        mySql_insert_query = "INSERT INTO material_articles (article_id, created_on, fullmaterial, hfb_id, material,name,pa_id,pra_id,sort,status) VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s,%s);"

        mycursor.execute(mySql_insert_query, a)
        #print(mycursor._last_executed)
        mydb.commit()
        
    os.remove('../pqreport/media/run/runscript_lock')
    

    '''
    df.rename(columns = {'ITEM_NO' : 'article_id', 'ITEM_NAME' : 'name', 'SALES_START':'sales_start', 'SALES_END':'sales_end', 'HFB_NO':'hfb_id', 'PA_NO':'pa_id','PRA_NO':'pra_id','status':'status' }, inplace = True)

    print(df)

    hostname=cfg['mysql']['host']
    dbname=cfg['mysql']['db']
    uname=cfg['mysql']['user']
    pwd=""
    mydb = mysql.connector.connect(
        host=cfg['mysql']['host'],
        database=cfg['mysql']['db'],
        user=cfg['mysql']['user'],
        password=''
    )
    mycursor = mydb.cursor()
    mycursor.execute(""" DELETE FROM material_articles """)
    mydb.commit()

    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                    .format(host=hostname, db=dbname, user=uname, pw=pwd))
    df.to_sql('material_articles', engine,if_exists='replace', index=False)

    #os.remove('../pqreport/media/run/runscript_lock')
    '''
'''
latestfile = allfiles[-1]
uniquefiles = list(set(allfiles))
uniquefiles.sort()
secondlastfile = uniquefiles[-2]

latestfiles = list(filter(lambda x: x.startswith(latestfile),filenames))
secondfiles = list(filter(lambda x: x.startswith(secondlastfile),filenames))

latestfiles.sort()
secondfiles.sort()

#print(os.walk('media/csv/'))
path1 = '../pqreport/media/test/'+str(latestfiles[-1])
path2 = '../pqreport/media/test/'+str(secondfiles[-1])
df1=pd.read_excel(path1).sort_values(by=['ITEM_NO','PRODTYPE_TEXT','PART_TEXT','MATERIAL_TEXT'], ascending=False)
df2=pd.read_excel(path2).sort_values(by=['ITEM_NO','PRODTYPE_TEXT','PART_TEXT','MATERIAL_TEXT'], ascending=False)



df1['material'] = df1['PRODTYPE_TEXT'].astype(str)+' '+df1['PART_TEXT'].astype(str)+' '+df1['MATERIAL_TEXT'].astype(str)
df2['material'] = df2['PRODTYPE_TEXT'].astype(str)+' '+df2['PART_TEXT'].astype(str)+' '+df2['MATERIAL_TEXT'].astype(str)

#df1['sortedcom'] = df1['PRODTYPE_TEXT'].astype(str)+' '+df1['PART_TEXT'].astype(str)+' '+df1['MATERIAL_TEXT'].astype(str)
#df2['sortedcom'] = df2['PRODTYPE_TEXT'].astype(str)+' '+df2['PART_TEXT'].astype(str)+' '+df2['MATERIAL_TEXT'].astype(str)
#print(df1)
df01 = df1.groupby('ITEM_NO').agg({
                        'ITEM_NAME':'first',
                        'HFB_NO':'first',
                        'PRA_NO':'first',
                        'PA_NO':'first',
                        'SALES_START':'first',
                        'SALES_END':'first', 
                        'material': ', '.join
                        }).sort_values('material', ascending=False).reset_index()

df02 = df2.groupby('ITEM_NO').agg({
                        'ITEM_NAME':'first',
                        'HFB_NO':'first',
                        'PRA_NO':'first',
                        'PA_NO':'first',
                        'SALES_START':'first',
                        'SALES_END':'first', 
                        'material': ', '.join
                        }).sort_values('material', ascending=False).reset_index()

df = df01[~(df01['ITEM_NO'].isin(df02['ITEM_NO']) & df01['material'].isin(df02['material']) )].reset_index(drop=True)
pd.set_option('display.max_columns', None)


df['change'] = df['ITEM_NO'].map(df02.set_index('ITEM_NO')['material']).fillna('')
df['id'] = df.index
#df['change'] = np.where( df['ITEM_NO'].isin(df02['ITEM_NO']) == True, df02.loc[df02['ITEM_NO']].['combined'].values[0], '')
df['fullmaterial'] = df['change'].str.replace('nan','')
df['material'] = df ['material'].str.replace('nan','')
now = datetime.now()    
formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
df['created_on'] = formatted_date
df['sort'] = formatted_date


df.rename(columns = {'ITEM_NO' : 'article_id', 'ITEM_NAME' : 'name', 'SALES_START':'sales_start', 'SALES_END':'sales_end', 'HFB_NO':'hfb_id', 'PA_NO':'pa_id','PRA_NO':'pra_id' }, inplace = True)

print(df)

hostname="localhost"
dbname="pqreport"
uname="root"
pwd=""
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
				.format(host=hostname, db=dbname, user=uname, pw=pwd))
df.to_sql('material_articles', engine,if_exists='replace', index=False)


'''

















'''
engine = create_engine('mysql://root:@localhost/test')
with engine.connect() as conn, conn.begin():
    df.to_sql('material_articles', conn, if_exists='replace')
'''
'''
with open("config.yml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

mydb = mysql.connector.connect(
        host=cfg['mysql']['host'],
        database='test',
        user=cfg['mysql']['user'],
        password=''
    )
mycursor = mydb.cursor()

df.to_sql(name='test', con=mydb, if_exists = 'replace', index=False)

print('asdasd')
'''
'''

t = []
for _, row in df.iterrows():
    if row[0] in df02['ITEM_NO'].values:
        chloc = df02.loc[(df02['ITEM_NO'] == row[0])]
        change = chloc['combined'].values[0]
        sort = 0
    else:
        sort = 1    
        change = ''
    ss = None if row[6] == pd.NaT else row[6]
    se = None if row[7] == pd.NaT else row[7]

    art = (int(row[0]),row[1].replace('nan',''),row[7].replace('nan',''),sort,str(ss),str(se),change.replace('nan',''),1,int(row[2]),int(row[3]),int(row[4]))
    
    t.append(art)
'''    
'''
mydb = mysql.connector.connect(
        host=cfg['mysql']['host'],
        database='test',
        user=cfg['mysql']['user'],
        password=''
    )
mycursor = mydb.cursor()
mySql_insert_query = "INSERT INTO material_articles (article_id, material, sort, sales_start, sales_end,created_on,fullmaterial,status,hfb_id,pa_id,pra_id) VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s);"

mycursor.execute(""" DELETE FROM material_articles """)
mydb.commit()

mycursor.executemany(mySql_insert_query, t)
mydb.commit()
'''
