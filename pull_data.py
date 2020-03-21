import requests
import datetime

base_url = 'https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow'
destination_path = './data/daily_data/'

def format_filename(destination,filename,date_string):
    fname_comp = filename.split('.')
    output_fname = '%s_%s.%s'%(fname_comp[0],date_string,fname_comp[1])
    output_path = destination + output_fname
    return output_path

def save_file(filepath,request_object):
    with open(filepath, 'wb') as f:
        f.write(request_object.content)
    print('Saved File to: %s'%filepath)
    
def execute_request(base_url, filename, destination, date_string):
    formatted_base_url = '%s/%s/%s'%(base_url,date_string[0:4],date_string)
    request_url = '%s/%s'%(formatted_base_url,filename)
    r = requests.get(request_url)
    if r.status_code == 200 and r.headers['content-type'] == 'application/octet-stream':
        filepath = format_filename(destination,filename,date_string)
        save_file(filepath,r)
        return True
    else:
        return False
        
start_date = datetime.date(2012,1,1) #NOTE: First daily_data.dat = 20131022, First daily_data_v2.dat = 20180718
end_date = start_date + datetime.timedelta(5)
#end_date = datetime.date.today() - datetime.timedelta(2)
current_date = start_date
while True:
    #format the date for the request
    request_date_string = current_date.strftime('%Y%m%d')
    
    #try an initial request with daily_data_v2.dat to get all the fields
    initial_success = execute_request(base_url,'daily_data_v2.dat',destination_path,request_date_string)
    if not initial_success:
        second_try_success = execute_request(base_url,'daily_data.dat',destination_path,request_date_string)
    if not second_try_success:
        print('No files available for: %s'%current_date.strftime('%m/%d/%Y'))
    
    if current_date == end_date:
        break
    else:
        current_date += datetime.timedelta(1)