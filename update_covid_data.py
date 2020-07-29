import urllib.request

def main( ):

    import_all_data_from_urls( )

    
    exit( )
    
def import_all_data_from_urls( ):

    # base URLs for API/Git pages
    atlanticAPIUrl = 'https://covidtracking.com/api/'
    nytGITUrl = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master'
    
    nytFilesDct = { 'nyt_us_counties.txt' : 'us-counties.csv' }
    atlanticFilesDct = { 'atl_historic_us.txt' : 'v1/us/daily.csv' }
    
    def make_data_requests_and_write( baseUrl, filePathDct, outputPath = 'csv_data' ):
        """ This subroutine makes the queries and writes the CSV data to a file. """
        # ARGUMENT baseUrl -> str basic URL path
        # ARGUMENT filePathDct -> str path to specific file
        # ARGUMENT outputPath -> path to output folder; DEFAULT
        
        # RETURN nothing
    
        # get data from each file
        for file, path in filePathDct.items( ):
        
            print( f'Requesting data for {file}...', end = ' ' )
    
            url = f'{baseUrl}/{path}'
            data = import_from_url( url )
            
            if data:
                
                with open( f'{outputPath}/{file}', 'w' ) as fileOut:
                    
                    for line in data:
                        print( line, file = fileOut )
                        
                print( 'YES' )
                
            else:
                print( 'NO' )
                
        return
            
    make_data_requests_and_write( nytGITUrl, nytFilesDct )
    make_data_requests_and_write( atlanticAPIUrl, atlanticFilesDct )

    return
        

def import_from_url( url, sinkShelf = 0):
    """ Get text from a web page with all line breaks removed. """
    # ARGUMENT url -> str internet address of the data
    # ARGUMENT sinkShelf -> int counter; default
    
    # RETURN
    lines = [ ] # list str HTML formated web page file lines
    
    while sinkShelf < 10:
        """ Try at least ten times to get the URL data. """
        try:
            f = urllib.request.urlopen( url )
            data = f.read( )
            lines = data.decode( 'utf-8').split( '\n' )
                
            return lines
            
        except:
            sinkShelf += 1
            
    return None
    
if __name__ == '__main__':
    main( )