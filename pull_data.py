import requests
import datetime

class AirDataExtractor(object):
    '''
    Purpose: extracts daily air quality readings files from AirNow.
    Constructor arguments:
        - base_url (str): common url for all requests, should not end with a /
        - destination_path (str): destination for data files. MUST end with a /
        - base_file_names (list: str): list of the base file names to look for
            on the AirNow server. The defaults included are for the daily summary
            data. daily_data_v2.dat and daily_data.dat. List the names in order
            of preference as the extractor will stop for a given date after the 
            first successful file.
        - start_date (datetime.date): First date to pull data for. Defaults to 
            the system date when this object is created.
        - end_date (datetime.date): Last date to pull data for. Defaults to the
            system date when this object is created.
    '''

    def __init__(self, 
                base_url, 
                destination_path, 
                base_file_names = ['daily_data_v2.dat', 'daily_data.dat'], 
                start_date = datetime.date.today(), 
                end_date = datetime.date.today()):
        self.base_url = base_url
        self.destination_path = destination_path
        self.base_file_names = base_file_names
        self.start_date = start_date
        self.end_date = end_date

    def format_filename(self, filename, date):
        '''
        Turns a base file name and a date into an output filename.
        '''
        f_comp = filename.split('.')
        output_fname = '%s_%s.%s'%(f_comp[0],date.strftime('%Y%m%d'),f_comp[1])
        return output_fname

    def format_url(self, filename, date):
        '''
        Generates a request url for the given filename and date.
        '''
        date_str = date.strftime('%Y%m%d')
        formatted_base_url = '%s/%s/%s'%(self.base_url,date_str[0:4],date_str)
        request_url = '%s/%s'%(formatted_base_url,filename)
        return request_url

    def save_file(self, filename, request):
        '''
        Saves the content returned by a request to a given filename at a given destination.
        '''
        filepath = self.destination_path + filename
        with open(filepath, 'wb') as f:
            f.write(request.content)
        print('Saved File to: %s'%filepath)

    def execute_request(self, url, filename):
        '''
        Retrieves the file at the specified url. If the resource is retrieved
        successfully and is of the correct type, saves the file and returns True. Returns False 
        if either of those conditionsare not met.
        '''
        request = requests.get(url)
        status_code = request.status_code
        content_type = request.headers['content-type']
        if status_code == 200 and content_type == 'application/octet-stream':
            self.save_file(filename,request)
            return True
        else:
            return False

    def extract(self):
        '''
        Attempts to extract files for the date range given by the class constructor.
        '''
        current_date = self.start_date
        while True:
            succeeded = False
            for base_file_name in self.base_file_names:
                url = self.format_url(base_file_name,current_date)
                filename = self.format_filename(base_file_name,current_date)
                if not succeeded:
                    succeeded = self.execute_request(url, filename)
                else:
                    break
            
            if not succeeded:
                print('No files available for: %s'%current_date.strftime('%m/%d/%Y'))

            if not current_date == end_date:
                current_date += datetime.timedelta(1)
            else:
                break

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-s",
                        "--start_date", 
                        type = str, 
                        help = "First date of data to extract, format like mm/dd/yyyy. Defaults to today.", 
                        default = datetime.date.today().strftime('%m/%d/%Y'))
    parser.add_argument("-e",
                        "--end_date", 
                        type = str, 
                        help = "Last date of data to extract, format like mm/dd/yyyy. Defaults to today.", 
                        default = datetime.date.today().strftime('%m/%d/%Y'))
    
    parser.add_argument("-i",
                        "--ignore",
                        help = "Ignores the end date, program will pull data for only the start date.",
                        action = "store_true")

    args = parser.parse_args()
    
    start_date = datetime.datetime.strptime(args.start_date,'%m/%d/%Y')

    if args.ignore:
        end_date = start_date
    else:
        end_date = datetime.datetime.strptime(args.end_date,'%m/%d/%Y')
    
    date_diff = end_date - start_date

    if date_diff.days >= 0: 
        
        base_url = 'https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow'
        destination_path = './data/daily_data/'
        #NOTE: First daily_data.dat = 20131022, First daily_data_v2.dat = 20180718
        #start_date = datetime.date(2013,10,22) 
        # End Date 2 days prior because today and yesterday files are updated multiple times per hour
        #end_date = datetime.date.today() - datetime.timedelta(2)
        
        extractor = AirDataExtractor(base_url,destination_path,start_date = start_date , end_date = end_date)
        extractor.extract()
    else:
        print("End date must be after start date. End date is currently %i days before start date."%date_diff.days)