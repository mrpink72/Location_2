import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import importlib
from datetime import datetime,timedelta,date
import configparser as cfg
import os
import sys
from termcolor import colored
import io
import requests as rq
import numpy as np
from dotenv import load_dotenv

import sharepoint as sp

run_dir = os.path.dirname(__file__) + '/'

load_dotenv()
a_client_id = os.getenv('a_client_id')
a_client_secret = os.getenv('a_client_secret')

c_client_id = os.getenv('c_client_id')
c_client_secret = os.getenv('c_client_secret')

hb_token = os.getenv('hibob')

conf = cfg.ConfigParser(interpolation=None)

try:
    conf.read(run_dir + 'config.ini')
    
    a_site_url = conf['AnalyticPortal']['site_url']

    c_site_url = conf['Controlling']['site_url']
    
    local = conf.getboolean('Dictionary','local')
    local_file = conf['Dictionary']['local_file'].replace('"','').replace("'",'')
    sp_file = conf['Dictionary']['sp_file'].replace('"','').replace("'",'')

    backup = conf['Default']['backup'].replace('"','').replace("'",'')
    loc_folder = conf['Default']['loc_folder'].replace('"','').replace("'",'')
    out_path = conf['Default']['out_path'].replace('"','').replace("'",'')
    basa_path = conf['Default']['basa_path'].replace('"','').replace("'",'')
    do_backup = conf.getboolean('Default','do_backup')
    show = conf.getboolean('Default','show')
    prn_log = conf.getboolean('Default','print_log')
    log_file = conf['Default']['log_file'].replace('"','').replace("'",'')
    # read data from IS
    is_data = conf['Default']['is_data'].replace('"','').replace("'",'')
    ip_loc = conf['Default']['ip_loc'].replace('"','').replace("'",'')
    access_data = conf['Default']['access_data'].replace('"','').replace("'",'')
    country_code = conf['Default']['country_code'].replace('"','').replace("'",'')
    exception = conf['Default']['exception'].replace('"','').replace("'",'')
    #hibob = conf['Default']['hibob'].replace('"','').replace("'",'')
    hb_url = conf['Hibob']['url'].replace('"','').replace("'",'')
    zones_fld = conf['Default']['zones'].replace('"','').replace("'",'')

except Exception as e:
    print(colored(f"WARNING! There is the problem with key {str(e)}!","red"))

prnt = False if len(sys.argv)>1 else True

log_file_name = os.path.basename(log_file)
log_file_path = os.path.dirname(log_file)

# create connections
sc = sp.Sharepoint(c_site_url,c_client_id,c_client_secret)
sa = sp.Sharepoint(a_site_url,a_client_id,a_client_secret)


#df_log = sc.read_single(log_file,format='csv')
df_log = pd.DataFrame(columns=['Time','Action'])

def log(msg,prn=True):
    if prn:
        print("= ", datetime.now().strftime("%Y.%m.%d %H:%M:%S") ,": "+msg)
    df_log.loc[len(df_log)] = [datetime.now().strftime("%Y.%m.%d %H:%M:%S"),msg]    

log("-= Start =-",prn_log)
        
#backup files
if do_backup:
    log("Backup files",prn_log)
    
    folder = datetime.now().strftime('%Y.%m.%d_%H_%M')
    # target_folder = sa.create_folder(backup,folder) + '/'
    target_folder = sc.create_folder(backup,folder) + '/'

    # sa.copy_folder(out_path, target_folder, show=show)
    sc.copy_folder(out_path, target_folder, show=show, progress=prnt)
    
    
# read and transform lacation files from Sharepoint
log("Read and transform location files from Sharepoint",prn_log)

f_path = loc_folder

df = sc.read_files(path=f_path, format='xls', sheet='Staff', show=show, filter='^\d{2}_', progress=prnt)
df['N_file'] = df['file'].str.split('_').str[0].astype(int)
df.drop('IP Location', axis=1, inplace=True)

#read data of BasaFTE for current date
log("Read data of BasaFTE for current date",prn_log)

df_base = sa.read_single(f'{basa_path}{datetime.now().strftime("%Y%m%d")}_BasaFTE_Location.csv',format='csv')

df_base = df_base[df_base['Category'].isin([2,7,8])]

df_base['Date_IN'] = df_base['Date_IN'].astype('datetime64[ns]')
df_base = df_base[['TabNom', 'Name', 'ManagerB1', 'UnitB1', 'UnitFullName','JobType', 'Position', 'Date_IN', 
                   'BusLine', 'BoardName','Level', 'EMail','Sex','MacroRegion','Login']].\
                       rename(columns={'TabNom':"Табельний номер",'Name':'ПІБ (повністю)','UnitB1':'Департамент','UnitFullName':'Підрозділ',
                                       'EMail':'E-mail','BusLine':'Бізнес лінія','JobType':'Характер праці','Position':'Посада',
                                       'Level':'Рівень посади','Date_IN':'Дата прийняття на роботу','Sex':'Стать','BoardName':'Board Member'})

df_bl = df_base[["Бізнес лінія",'Board Member']].drop_duplicates()

headers = {
    "accept":"application/json", 
    "Authorization":"Basic " + hb_token 
}
response = rq.get(hb_url, headers=headers)

df_hi = pd.read_csv(io.StringIO(response.text))
df_hi['Start date'] = pd.to_datetime(df_hi['Start date'])
df_hi['ID'] = df_hi['Email'].apply(lambda s: abs(hash(s)) % (10 ** 10))
df_hi = df_hi[['ID','Email','Last name UA', 'First name UA', 'Middle name UA', 'Department', 'Employment type', 'Job title', 'Start date', 
                  'Business line','Seniority', 'Gender','User ID','Status']]

df_hi = df_hi[~(df_hi['Employment type'].str.contains('Internal staff',na=False)) & (df_hi['Status']=='Active')]
df_hi['Department'] = df_hi['Department'].str.lower().str.replace('it','ІТ').str.replace('іт','ІТ').str.replace('agile','Agile')
df_hi['Name'] = df_hi['Last name UA'] + ' ' + df_hi['First name UA'] + ' ' + df_hi['Middle name UA'].fillna("")

df_hi['Підрозділ'] = 'UDC'
df_hi['MacroRegion'] = 'UDC'
df_hi['Gender'] = df_hi['Gender'].apply(lambda x: 'M' if x=='Male' else 'W')

df_hi = df_hi[['ID','Email','Name', 'Department', 'Employment type', 'Job title', 'Start date','Business line','Seniority', 'Gender','User ID',
               'Підрозділ','MacroRegion']].\
                rename(columns={'ID':"Табельний номер",'Name':'ПІБ (повністю)','Department':'Департамент',
                                       'Email':'E-mail','Business line':'Бізнес лінія','Employment type':'Характер праці','Job title':'Посада',
                                       'Seniority':'Рівень посади','Start date':'Дата прийняття на роботу','Gender':'Стать',"User ID":'Login'})

df_hi = df_hi.merge(df_bl,how='left',on='Бізнес лінія')
df_hi.loc[(df_hi['Board Member'].isna()) & (df_hi['Департамент'].str.contains("ІТ")), 'Board Member'] = "Enkelejd ZOTAJ"
df_hi.loc[(df_hi['Бізнес лінія'].isna()) & (df_hi['Департамент'].str.contains("ІТ")), 'Бізнес лінія'] = "IT"

df_base = pd.concat([df_hi,df_base],axis=0)

#read all dictionary
log("Read all dictionary",prn_log)

if local:
    df_dict = pd.read_excel('dict.xlsx',sheet_name=['Country','Region','Distr','Unit_file'])
    country_list = df_dict['Country']['Country'].values.tolist()
    region_list = df_dict['Region']['Region'].values.tolist()
    distr_list = df_dict['Distr']['Physical Location'].values.tolist()

    df_unit = df_dict['Unit_file'].groupby(['# file','name'])['Unit B1'].unique().to_frame().reset_index().set_index('# file')    
else:    
    #sa = sp.Sharepoint(a_site_url,a_client_id,a_client_secret)
    country_list = sa.read_single(sp_file,format='xls',sheet='Country', show=False)['Country'].values.tolist()
    region_list = sa.read_single(sp_file,format='xls',sheet='Region', show=False)['Region'].values.tolist()
    distr_list = sa.read_single(sp_file,format='xls',sheet='Distr', show=False)['Physical Location'].values.tolist()
     
    df_unit = sa.read_single(sp_file,format='xls',sheet='Unit_file', show=False)
    unit_list = df_unit.loc[~df_unit['Unit B1'].isna(), 'Unit B1'].values.tolist()
    df_unit = df_unit.groupby(['# file','name'])['Unit B1'].unique().apply(list).reset_index().set_index('# file')
    
    df_zones = sa.read_single(zones_fld + "Zones.xlsx",format='xls', show=False)
    df_zones = df_zones[list(df_zones.keys())[0]]
    df_greenz = sa.read_single(zones_fld + "Green Fixed Zones.xlsx",format='xls',sheet='Green_Fix', show=False)
     
    df_updtz = df.merge(df_zones[['E-mail','Zone']],on='E-mail',how='left')
    df_updtz = df_updtz.merge(df_greenz,how='left',left_on='Область / Region', right_on='Region').drop_duplicates()
    df_updtz['Zone'] = np.where(df_updtz['Zone_y'].isna(),df_updtz['Zone_x'],df_updtz['Zone_y'])#.astype(int)
    df_updtz = df_updtz[['file','Табельний номер','Країна / Country','Область / Region','Місто / Село','E-mail','Zone']]
    df_updtz.rename({'file':'File name','Табельний номер':'TabNom','Країна / Country':'Country',
                     'Область / Region':'Region','Місто / Село':'City'},axis=1, inplace=True)
    
    bytes_file_obj = io.BytesIO()
    df_updtz.to_excel(bytes_file_obj,index=False)
    bytes_file_obj.seek(0)
    sa.upload_buf(bytes_file_obj,zones_fld,'Zones.xlsx',show=False)    
    

# Блок обробки логів Zscaler та VPN
warnings.filterwarnings('ignore', message='Unverified HTTPS request')
def save_data(df, type):
    bytes_file_obj = io.BytesIO()
    df.to_csv(bytes_file_obj,index=False, encoding='utf-8-sig')
    bytes_file_obj.seek(0)
    sa.upload_buf(bytes_file_obj,ip_loc,type + '_data.csv',show=False)
    
def cash_location(df_all,df):
    if 'IP Address' in df.columns:
            df.rename(columns={'IP Address':'Source Address'},inplace=True)
    if len(df_all)>0 :
        df_ip = df_all.loc[df_all['Source Address'].isin(df['Source Address']),['Source Address','Site']].drop_duplicates()
        df = df.merge(df_ip,on='Source Address',how='left')
        df_all = pd.concat([df_all,df.loc[~df['Site'].isna(),['Login', 'EmplName', 'Source Address', 'Date', 'Site']]])
        # print(df_all)
        df = df.loc[df['Site'].isna(),['Login', 'EmplName', 'Source Address', 'Date', 'Site']]
    return df_all,df    

def get_site(df:pd.DataFrame) -> pd.DataFrame:
    # df=df.head(3000)
    header = {'Authorization': 'Token e9bcba33143e7c96a2029b411343c3e65bf5f708',
            'Accept': 'application/json',}
    ip_dict = {'Ind':[],'Site':[]}
    with rq.session() as session:
        for i,ip in df['Source Address'].items():
            if ip.startswith('172'):
                res = 'WiFi network'
            else:     
                param = {'q': ip}
                respond = session.get('https://netbox.kv.aval/api/ipam/prefixes/',headers=header,params=param,verify=False)
                if respond.status_code != 200:
                    res = 'Unknown'
                else:
                    res_list = respond.json()['results']
                    res =  res_list[len(res_list)-1]['site']['name'] if res_list[len(res_list)-1]['site']!=None else res_list[len(res_list)-2]['description']
            # print({'Ind':i,'Site':res})
            # print(datetime.now(),i,ip,res)
            ip_dict['Ind'] += [i]
            ip_dict['Site'] += [res]
            res = None    
    df_ip = pd.DataFrame.from_dict(ip_dict,orient='columns')
    df_ip.set_index('Ind',drop=True,inplace=True)  
    df = df.merge(df_ip,how='left',left_index=True,right_index=True).rename(columns={'Site_y':'Site'}).drop(columns=['Site_x'])
    return df

def get_new_files(type:str, path_data:str):
    type = type.lower()
    try:
        if type=='mybank':
            df_arc = sa.read_single(ip_loc + 'office_data.csv',format='csv',show=show)
        else:    
            df_arc = sa.read_single(ip_loc + type + '_data.csv',format='csv',show=show)
        df_arc['Date'] = df_arc['Date'].str[:10].astype('datetime64[ns]') 
        date_f = df_arc['Date'].max()
        if type=='mybank':
            df_arc = None
    except:
        df_arc = pd.DataFrame()    
        date_f = date(2020,1,1)
    #print("Data sience date:",date_f)    
    f = sa.get_folder_files(path_data)
    df_tmp = pd.DataFrame()
    df_tmp['path'] = [file.serverRelativeUrl for file in f]
    df_tmp['file'] = [file.name for file in f]
    days = 0 if type.startswith("build") else 1
    df_tmp['date'] = df_tmp['file'].str.extract('(\d+-\d+-\d+)', expand=True).astype('datetime64[ns]') - timedelta(days=days)
    df_tmp['type'] = df_tmp['file'].str.lower().str[:3]   
    df_tmp['date'].fillna('2020-01-02',inplace=True) 
    f_list = df_tmp[(df_tmp['type']== type[:3])&(df_tmp['date']>pd.to_datetime(date_f))]['path'].to_list()
    df_res = sa.read_files(path_data,list=f_list,show=show, progress=prnt,format='csv')
    if len(df_res)>0 and type.startswith("build"):
        df_res['Date'] = pd.to_datetime(df_res['Date'])#, format='%d.%m.%Y')
    if len(df_res)>0 and not type.startswith("build"):
        df_res['Date'] = df_res['file'].str.extract('(\d+-\d+-\d+)', expand=True).astype('datetime64[ns]') - timedelta(days=1)
        if type.startswith("office"):
            df_arc, df_res =cash_location(df_arc,df_res)
            df_res = get_site(df_res)
        if type.startswith("mybank"):
            df_res = df_res.merge(df_base[['E-mail','Login','ПІБ (повністю)']],left_on='Source User Name',right_on='E-mail')
            df_res = df_res[['Login','ПІБ (повністю)','Source Address','Date']].rename(columns={'ПІБ (повністю)':'EmplName'})
            df_res['Site'] = 'WiFi network'
            return df_res    
    df_res = pd.concat([df_arc,df_res])
    return df_res

log("Read VPN logs",prn_log)
df_vpn = get_new_files('vpn',is_data)
save_data(df_vpn[['Login','Country','Date']],'vpn')

log("Read Zscaler logs",prn_log)
df_zs = get_new_files('zscaler',is_data)
save_data(df_zs[['user','ClientCountry','Date']],'zscaler')
df_zs.loc[df_zs['ClientCountry'].isin(["PL UA","DE PL UA","AT UA"]),"ClientCountry"] = "UA"

log("Read Office logs",prn_log)
df_office = get_new_files('office',is_data)
df_office = df_office[['Login','EmplName','Source Address','Date','Site']]

log("Read MyBank data",prn_log)
df_mybank = get_new_files('mybank',is_data)
df_office = pd.concat([df_office,df_mybank])

save_data(df_office,'office')

log("Read Building Access data",prn_log)

df_access = get_new_files('buildingaccess',access_data)
save_data(df_access[['TabNom','Name','Campus','Date']],'buildingaccess')

df_code = sa.read_single(country_code,format='xls', sheet='in', show=show)
df_exc = sa.read_single(exception,format='xls', sheet='Sheet1', show=show)
df_office = df_office[~df_office['Source Address'].isin(df_exc['IP'])]

df_vpn = df_vpn.merge(df_base[['Login','E-mail']],how='left',left_on='Login',right_on='Login').dropna(subset=['E-mail'])[['E-mail','Country','Date']]

df_zs = df_zs[~df_zs['ClientCountry'].isna()]
df_zs['ClientCountry'] = df_zs['ClientCountry'].apply(lambda x: x.split(" ")[0] if type(x)!=float else 'UA')
df_zs = df_zs.merge(df_code[['alpha-2','name']],how='left',left_on='ClientCountry',right_on='alpha-2')[['user','name','Date']]
df_zs.columns = ['EMail','Country','Date']

df_office = df_office.merge(df_base[['Login','E-mail']],how='left',left_on='Login',right_on='Login').dropna(subset=['E-mail'])[['E-mail','Date']].drop_duplicates()
df_office['Country']='Ukraine'

df_access = df_access.merge(df_base[['Табельний номер','E-mail']],how='left',left_on='TabNom',right_on='Табельний номер').dropna(subset=['E-mail'])[['E-mail','Campus','Date']].drop_duplicates()
df_access['Country']='Ukraine'
df_access[df_access['E-mail'].isna()]

df_loc = df_zs.merge(df_vpn,how='outer',left_on=['EMail','Date'],right_on=['E-mail','Date'])#.dropna(subset=['Country_y'])
df_loc['Country_x'].fillna(df_loc['Country_y'],inplace=True)

df_loc = df_loc.merge(df_office,how='outer',left_on=['EMail','Date'],right_on=['E-mail','Date'])#.dropna(subset=['Country'])
df_loc['Country_x'].fillna(df_loc['Country'],inplace=True)

df_loc.drop(columns=['Country_y','Country'],inplace=True)
df_loc = df_loc.merge(df_access,how='outer',left_on=['EMail','Date'],right_on=['E-mail','Date'])
df_loc['Country_x'].fillna(df_loc['Country'],inplace=True)

df_loc = df_loc[['EMail','Country_x','Date']].drop_duplicates()
df_loc.rename(columns={'Country_x':'IP Location'},inplace=True)
df_loc = df_loc.loc[df_loc['Date'].dt.year>=2024]
# df_loc=df_loc.loc[df_loc['Date'].dt.weekday<5]

#print(df_office.memory_usage())
del df_office
del df_access
del df_vpn
del df_zs

#print(df_loc.info())

df_norm_loc = df_loc.groupby(['Date','EMail'])['IP Location']
df_norm_loc = df_norm_loc.unique()
df_norm_loc = df_norm_loc.apply(list)
df_norm_loc = df_norm_loc.reset_index()

df_norm_loc['IP Location'] = df_norm_loc['IP Location'].apply(lambda x: [y for y in x if str(y)!="nan"])
id_multi = df_norm_loc.loc[df_norm_loc['IP Location'].apply(lambda x: len(x)>1)].index
id_one = df_norm_loc.loc[df_norm_loc['IP Location'].apply(lambda x: len(x)==1)].index

df_norm_loc.loc[id_multi,'IP Location'] = df_norm_loc.loc[id_multi]['IP Location'].apply(lambda x: next(y for y in x if y!="Ukraine"))
df_norm_loc.loc[id_one,'IP Location'] = df_norm_loc.loc[id_one]['IP Location'].apply(lambda x: x[0]) 
df_norm_loc['IP Location'].replace({'United Kingdom':'Great Britain', 'Czechia':'Czech Republic','Russian Federation':'Russia'},inplace=True)
df_norm_loc.sort_values(by=['Date','EMail'],inplace=True)
df_norm_loc = df_norm_loc.groupby(['EMail']).agg({'IP Location':'last'}).reset_index()

df = df.merge(df_norm_loc,how='left',left_on='E-mail',right_on='EMail')
df.drop('EMail',axis=1,inplace=True)

# df.to_excel('output.xlsx',index=False)


def create_file(nom,name,df):
    sheet_name = 'Staff'
    out_file = format(nom,'02') + '_' + name + '.xlsx'
    path = run_dir +  'out/'
    
    writer = pd.ExcelWriter(path + out_file, engine='xlsxwriter', datetime_format='DD.MM.YYYY')
    df.to_excel(writer, sheet_name=sheet_name, index=False)
    (max_row, max_col) = df.shape
    if max_row == 0:
        max_row = 1

    workbook = writer.book
    worksheet = writer.sheets[sheet_name]

    header_format = workbook.add_format({
        'bold': True,
        'fg_color': 'yellow',
        'font_color': 'black',
        'text_wrap': True,  
        'valign': 'vcenter',
        'align':'center',  
        'border': 1 
    })
    format_phone = workbook.add_format({'num_format': '#0'})
    
    column_settings = []
    for header in df.columns:
        column_settings.append({'header': header})

    worksheet.add_table(0,0,max_row,max_col-1,{'columns':column_settings,
                                               'style': 'Table Style Light 21'})
    worksheet.set_row(0, 40)
    # worksheet.autofilter(0,0,0,len(df.columns))
    for col_num, value in enumerate(df.columns.values):    
        worksheet.write(0, col_num, value, header_format)
        column_len = df[value].astype(str).str.len().max()
        column_len = max(column_len if str(column_len)!='nan' else len(value)/2+5, len(value)/2+5)
        if col_num in [10,11]:
            worksheet.set_column(col_num, col_num, column_len+2, format_phone)
        else:    
            worksheet.set_column(col_num, col_num, column_len)

    worksheet.data_validation(1,6,max_row,6, {'validate': 'list',
                                    'source': ['YES','NO','BCM']
                                    })    

    dict_sheet = workbook.add_worksheet('Dict')
    for i in range(len(country_list)):
        dict_sheet.write(i+1,0,country_list[i])
    
    worksheet.data_validation(1,8,max_row,8, {'validate': 'list',
                                    'source': f'Dict!$A$1:$A${len(country_list)+1}'
                                    })  

    for i in range(len(region_list)):
        dict_sheet.write(i+1,1,region_list[i])
        
    worksheet.data_validation(1,9,max_row,9, {'validate': 'list',
                                    'source': f'Dict!$B$1:$B${len(region_list)+1}'
                                    })      
        
    for i in range(len(distr_list)):
        dict_sheet.write(i+1,2,distr_list[i])    
        
        
    worksheet.data_validation(1,10,max_row,10, {'validate': 'list',
                                    'source': f'Dict!$C$1:$C${len(distr_list)+1}'
                                    }) 

    workbook.close()
    return path + out_file

def progress(cur, total):
    dec_num = cur/(total/10)
    print('▓' * int(dec_num), f'{int(dec_num*10)}%', end='\r')
    return 

def staff_match(i):
    unit, name = df_unit.loc[[i]][['Unit B1','name']].values[0].tolist()
    # print(f"{i:02d}_{name}")
    if i == 99:
        df_new = df_base[~df_base['Департамент'].isin(unit_list)] 
    else:
        df_new = df_base[df_base['Департамент'].isin(unit)]       
    df_new = df_new.merge(df[['E-mail','Mobile phone','Work status','IP Location','Країна / Country','Область / Region','Район / District','Місто / Село']],\
            on='E-mail', how='left')
    df_ = df_new[df_new['Країна / Country'].isna() | (df_new['Країна / Country']=='------')]
    df_new = df_new[(~df_new['Країна / Country'].isna()) & (df_new['Країна / Country']!='------')].sort_values('ПІБ (повністю)')
    df_[['Країна / Country','Область / Region']] = ['------','------']
    df_new = pd.concat([df_new,df_],ignore_index=True)
    df_new = df_new[['Табельний номер','ПІБ (повністю)','MacroRegion','ManagerB1','Департамент','Характер праці',
                        'Work status','IP Location','Країна / Country','Область / Region','Район / District','Місто / Село',
                        'Mobile phone',
                        'Підрозділ','Посада','Дата прийняття на роботу',
                        'Бізнес лінія','Board Member','Рівень посади','E-mail','Стать']]

    df_new.loc[(df_new['Область / Region']=='мобілізований') & (df_new['Характер праці']!='Мобілізований'), 'Область / Region'] = '------'
    df_new.loc[df_new['Характер праці'].isin(['Мобілізований','Служба ЗСУ']),'Область / Region'] = 'мобілізований'

    return create_file(i,name,df_new.drop_duplicates())

log("Create files",prn_log)

# out_files = []
del_file = True
n = 1
for i in df_unit.index:
    if prnt:
        progress(n,len(df_unit))
    f_name = staff_match(i)
    # sc.upload_file(f_name,out_path,show=show)
    if del_file:
        os.remove(f_name)
    n += 1    
  

log("-= Finish =-",prn_log)

# bytes_file_obj = io.BytesIO()
# df_log.to_csv(bytes_file_obj,index=False)
# bytes_file_obj.seek(0)
# sc.upload_buf(bytes_file_obj,log_file_path,log_file_name,show=False)
