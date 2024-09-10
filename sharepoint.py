

import pandas as pd

from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.files.file import File
from office365.sharepoint.utilities.move_copy_util import MoveCopyUtil
from office365.sharepoint.listitems.listitem import ListItem

import io
import os
import re

import datetime
import json
import warnings


class Sharepoint:
    def __init__(self,site_url,client_id,client_secret):
        self.site_url = site_url[:-1] if site_url[-1]=='/' else site_url
        self.ctx = ClientContext(self.site_url).with_credentials(ClientCredential(client_id,client_secret))
        def custom_header(request):
            request.headers['Prefer'] = 'bypass-shared-lock'
        #    request.verify = False
        #warnings.filterwarnings("ignore", message="Unverified HTTPS request")    
        self.ctx.before_execute(custom_header)
        
    def __fix_path(self,path):
        path = path.replace("https://rbinternational.sharepoint.com", "")
        if path[0] != '/':
            path = '/' + path
        return path        
    
    def progress(self, cur, total):
        dec_num = cur/(total/10)
        print('▓' * int(dec_num), f'{int(dec_num*10)}%', end='\r')
        return          

    def get_folder_files(self, path):
        path = self.__fix_path(path)
        files = self.ctx.web.get_folder_by_server_relative_url(path).files#.get().execute_query()
        self.ctx.load(files)
        self.ctx.execute_query()
        return files

    def read_files(self, path, format=['xls','csv'], filter=None,list=None, show=False, sheet=None, encoding=None, progress=True, reset_index=True) -> pd.DataFrame:
        df = pd.DataFrame()
        if list==None:
            files = self.get_folder_files(path)
            filter = filter if filter else '*'
            f_list = [file.serverRelativeUrl for file in files if re.match(filter, file.name)] #if filter else [file.serverRelativeUrl for file in files]
        else:
            f_list=list
        i=1
        for f in f_list:
            if show :
                print(f"{f}: filter='{filter}'")  
            elif progress:
                self.progress(i,len(f_list))      
            resp = File.open_binary(self.ctx, f)
            bytes_file_obj = io.BytesIO()
            bytes_file_obj.write(resp.content)
            bytes_file_obj.seek(0) #set file object to start
            if format.lower() =='xls':
                df_ = pd.read_excel(bytes_file_obj,sheet_name=sheet)
            elif format.lower()=='csv':
                if bytes_file_obj.getbuffer().nbytes > 10: 
                    df_ = pd.read_csv(bytes_file_obj, encoding=encoding)   
                else:
                    df_ = pd.DataFrame()
            else:
                raise Exception("Please, choose either XLS or CSV format")
            df_['file'] = f.split("/")[-1]
            df = pd.concat([df,df_])
            i = i + 1  
        if reset_index:
            df.reset_index(drop=True,inplace=True)    
        return df
      
    def read_single(self, file_url, format=['xls','xlsx','csv'], sheet=None, show=None, encoding=None):
        df = pd.DataFrame()
        file = self.__fix_path(file_url)
        if show:
            print(file,format)
        resp = File.open_binary(self.ctx, file)
        bytes_file_obj = io.BytesIO()
        bytes_file_obj.write(resp.content)
        bytes_file_obj.seek(0) #set file object to start
        if format.lower() in ['xls','xlsx']:
            df = pd.read_excel(bytes_file_obj,sheet_name=sheet)
        elif format.lower()=='csv':
            df = pd.read_csv(bytes_file_obj, encoding=encoding, sep=',')    
        else:
            raise Exception("Please, choose either XLS or CSV format")
        return df  
    
    def upload_file(self,file, put_path, show=False):
        put_path = self.__fix_path(put_path)
        with open(file, 'rb') as content_file:
            file_content = content_file.read()
        
        target_folder = self.ctx.web.get_folder_by_server_relative_url(put_path)
        name = os.path.basename(file)
        try:
            target_file = target_folder.upload_file(name, file_content).execute_query()
        except Exception as e:
            print(str(e))
            print(f"WARNING! File '{name}' has NOT been uploaded!!! Possibly it is opened by an another user!","red")    
        if show:
            print(f"File '{name}' has been uploaded to url: {format(target_file.serverRelativeUrl)}")

    def upload_buf(self,buf, put_path, name, show=False):
        """
            Uploads a file buffer to a target folder in SharePoint.

            Args:
                buf (bytes): The file buffer to upload.
                put_path (str): The server-relative URL of the target folder.
                name (str): The name of the file to upload.
                show (bool, optional): Whether to print the upload status. Defaults to False.

            Returns:
                None
        """
        put_path = self.__fix_path(put_path)
        target_folder = self.ctx.web.get_folder_by_server_relative_url(put_path)
        try:
            target_file = target_folder.upload_file(name, buf).execute_query()
        except Exception as e:
            print(str(e))
            print(f"WARNING! File '{name}' has NOT been uploaded!!! Possibly it is opened by an another user!","red")    
        if show:
            print(f"File '{name}' has been uploaded to url: {format(target_file.serverRelativeUrl)}")
 
    def get_list_items(self, list, view):
        """Get data from sharepoint list
            Version 1.0
            This version supports only one lookup field in a view
        Args:
            list (str): name of sharepoint list
            view (str): name of view of list

        Returns:
            DataFrame: data of list from sharepoint
        """
        sp_list = self.ctx.web.lists.get_by_title(list)
   
        fields = self.get_list_fields(sp_list, view)
        #f.get_property("EntityPropertyName"): [f.title,f.type_as_string] for f in fields
        fld = dict()
        fld["ID"] = "ID"
        title = ["ID"]
        for f in fields:
            if f.type_as_string == 'User':
                i = sp_list.get_lookup_field_choices(f.get_property("EntityPropertyName")).execute_query()
                lookup_data = pd.json_normalize(json.loads(i.value),record_path =['choices'])
                lookup_data.columns = ["ID",f.title]
            else:
                fld[f.get_property("EntityPropertyName")]=f.title  
            title.append(f.title)
 
        items = sp_list.items.select([*fld.keys()]).get().execute_query()
        arr = []
        for item in items:
            arr.append([item.properties[f] for f in [*fld.keys()]])
        df_items = pd.DataFrame(arr,columns=[*fld.values()])

        df_res = df_items.merge(lookup_data, on='ID', how='left') 
        
        return df_res[title].set_index('ID')

           
    def get_list_fields(self, list, view):  
        view_fields = list.views.get_by_title(view).view_fields.get().execute_query()

        fields = [list.fields.get_by_internal_name_or_title(field_name).get() for field_name in view_fields]
        self.ctx.execute_batch()   # From performance perspective i would prefer execute_batch over execute_query here

        return fields
    
    def create_folder(self, path, folder):
        path = self.__fix_path(path)
        target_folder = self.ctx.web.get_folder_by_server_relative_url(path).folders.add(folder).execute_query()
    
        return target_folder.serverRelativeUrl
    
    def copy_folder(self, source, target, show=False, progress=True):
        source = self.__fix_path(source)
        target = self.__fix_path(target)
        if show:
            print(source,target)
        source_folder = self.ctx.web.get_folder_by_server_relative_url(source)
        i = 1
        _files = source_folder.get_files().execute_query()
        for f in _files:
            # print(f.get_property('serverRelativeUrl'))
            name = f.get_property('name')
            if show:
                print(name)
            elif progress:
                self.progress(i,len(_files))     
            File.copyto(f,target+name,overwrite=True).get().execute_query()
            i = i + 1
        # target_folder = source_folder.copy_to(target, keep_both=False).get().execute_query()
        # MoveCopyUtil.copy_folder(self.ctx,source,target,options=None).execute_query()

        return #target_folder
    
    def get_file_content(self, file_url):
        file = self.__fix_path(file_url)
        resp = File.open_binary(self.ctx, file)
        return resp.content