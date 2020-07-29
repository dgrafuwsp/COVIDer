# update_covid_data
# by Danie L . Graf <dgraf@uwsp.edu>
""" This script gets the set of COVID data files from the NY Times and The Atlantic
web sites and converts the CSVs to tab-delimited tables. """
import urllib.request

class Covid_Data( ):
    """ This object stores the basic information about the CSV files from data
    sources. """

    def __init__( self, fileName, csvFileName ):
        """ Initializes the object. """
    
        self.file_name = fileName
        self.csv = csvFileName
        self.fields = [ ]
        self.data = [ ]
        
        return
        
    def brief_report( self ):
        """ Creates a short report (5 data lines) for a data set. """
    
        print( self.file_name )
        print( self.csv )
        print( self.fields )
        for d in self.data[ : 5 ]:
            print( d )
            
        print( )
        
        return

def main( ):

    # list of COVID data
    filesFields = [ Covid_Data( 'nyt_us_counties', 'us-counties.csv' ),
                    Covid_Data( 'nyt_us_states', 'us-states.csv' ),
                    Covid_Data( 'nyt_us', 'us.csv' ),
                    Covid_Data( 'nyt_mask_use', 'mask-use/mask-use-by-county.csv' ),
                    Covid_Data( 'nyt_excess_deaths', 'excess-deaths/deaths.csv' ),
                    
                    Covid_Data( 'atl_historic_us', 'v1/us/daily.csv' ),
                    Covid_Data( 'atl_historic_states', 'v1/states/daily.csv' ) ] 

    import_all_data_from_urls( filesFields )
    make_tab_delimited_tables( filesFields )
    
    exit( )
    
def make_tab_delimited_tables( filesFields, outputPath = 'covid_data' ):
    """ This function uses the CSV-formatted files to make tab-delimited tables. """
    # ARGUMENT filesFields -> ref to list of Covid_Date( ) objects
    # ARGUMENT outputPath -> str file path to write; DEFAULT
    
    # RETURN nothing (filesFields is mutable)

    def get_csv_data( ff, path = 'csv_data' ):
        """ This subroutine gets the CSV-formatted data from a file if it hasn't
        already been stored in a Covid_Data( ) object. """
        # ARGUMENT ff -> ref to Covid_Data( ) object
        # ARGUMENT path -> str path to CSV-formatted files
        
        # RETURN nothing (ff is mutable)
        
        # read the data from the file
        with open( f'{path}/{ff.file_name}.csv' ) as fileIn:
            lines = fileIn.read( ).split( '\n' )
        
        # update the Covid_Data( ) object
        ff.fields = lines[ 0 ].split( ',' )
        ff.data = [ line.split( ',' ) for line in lines[ 1 : ]
                        if line ]
        
        return

    # traverse the list of Covid_Data( ) objects
    for ff in filesFields:
    
        # if the data aren't available from being recently downloaded, then
        # retreive them from a file
        if not ff.data or not ff.fields:
            get_csv_data( ff )
        
        # write the data as a tab-delimited table    
        with open( f'{outputPath}/{ff.file_name}.txt', 'w' ) as fileOut:
        
            print( '\t'.join( ff.fields ), file = fileOut )
            
            for line in ff.data:
                print( '\t'.join( line ), file = fileOut )
    
    return
    
def import_all_data_from_urls( filesFields, outputPath = 'csv_data' ):
    """ This is the function that retrieves the data for each file. """
    # ARGUMENT filesFields -> ref to list of Covid_Date( ) objects
    # ARGUMENT outputPath -> str path to output folder; DEFAULT
    
    # RETURN nothing (filesFields is mutable)

    print( 'UPDATE THE COVID DATA CSV FILES' )
    
    # base URLs for API/Git pages
    baseUrlsDct = { 'nyt' : 'https://raw.githubusercontent.com/nytimes/covid-19-data/master',
                    'atl' : 'https://covidtracking.com/api',
                    'usc' : 'http://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/totals' }
    
    # get data from each file
    for ff in filesFields:
    
        print( f'Requesting data for {ff.file_name}...', end = ' ' )
        
        # get path to data
        filePrefix = ff.file_name[ : 3 ]
        baseUrl = baseUrlsDct[ filePrefix ]
        path = ff.csv
        url = f'{baseUrl}/{path}'
                
        # get data from the internet
        lines = import_from_url( url )
        
        # if there is data then write it to the text file
        if lines:
            
            with open( f'{outputPath}/{ff.file_name}.csv', 'w' ) as fileOut:
                for line in lines:
                    print( line, file = fileOut )
                    
            # get fields and parsed data
            ff.fields = lines[ 0 ].split( ',' )
            ff.data = [ line.split( ',' ) for line in lines[ 1 : ]
                                if line ]
                    
            print( 'YES' )  # report success!!
            
        else:
            print( 'NO' )   # report no update obtained
    
    print( ); print( )
    
    return
        
def import_from_url( url, sinkShelf = 0):
    """ Get text from a web page with all line breaks removed. """
    # ARGUMENT url -> str internet address of the data
    # ARGUMENT sinkShelf -> int counter for attempts; DEFAULT
    
    # RETURN
    lines = [ ] # list str HTML formated web page file lines
    
    # make at least ten requests for data before giving up
    while sinkShelf < 10:

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