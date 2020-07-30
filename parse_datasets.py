# parse_datasets
# by Daniel L. Graf <dgraf@uwsp.edu>
""" The purpose of this script is to parse larger COVID datasets down to more
manageable sizes. 

The NY Times and The Atlantic files to be parsed were created by the update script 
and stored in the /covid_data/ directory. The cenus data (in the same directory) is
downloaded from 'https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/totals/co-est2019-alldata.csv'
saved as 'usc_counties_2019.txt'. """

def main( ):

    print( 'PARSE THE COVID DATA FILES' )

    # The counties to be sampled are specified as a list of ( State, County )
    # tuples. An entry of None for either means that no entries will be filtered.
    
    # For example, ( 'Wisconsin', None ) would return all counties in Wisconsin.
    # For another, ( None, Chippewa ) would return all Chippewa counties in every
    # state. 
    countyCriteria = [ ( 'California', 'Los Angeles' ), ( 'Wisconsin', None ) ]

    lines = parse_nyt_county_data_by_date( countyCriteria )
    export_lines_to_file( lines, 'county_cases_and_deaths.txt' )
    
    lines = parse_census_county_data( countyCriteria )
    export_lines_to_file( lines, 'county_cenus_population.txt' )
    
    # The states to be sampled are specified as a list of states. An entry of None
    # means no entries will be filtered.
    stateCriteria = [ state for ( state, county ) in countyCriteria ]
    lines = parse_atlantic_states_data( stateCriteria )
    export_lines_to_file( lines, 'state_cases_and_deaths.txt' )
    
    print( ); print( )
    
    exit( )
    
def parse_atlantic_states_data( statesCriteria, 
                                usPath = 'covid_data/atl_historic_us.txt', 
                                statesPath = 'covid_data/atl_historic_states.txt' ):
    """ This function reduces the full The Atlantic states and USA datasets down to a
    table of results by date. """
    # ARGUMENT statesCriteria -> list of str states to limit dataset
    # ARGUMENT usPath -> str path to The Atlantic USA data
    # ARGUMENT statesPath -> str path to The Atlantic states data
    
    # RETURN                            
    output = [ ]    # list of str tab-delimited data
    
    print( 'Parse USA and states data from The Atlantic...', end = ' ' )
    
    # get us data
    usData = import_data_file( usPath )
    
    # {'date': 0, 'states': 1, 'positive': 2, 'negative': 3, 'pending': 4,
    # 'hospitalizedCurrently': 5, 'hospitalizedCumulative': 6, 'inIcuCurrently': 7, 
    # 'inIcuCumulative': 8, 'onVentilatorCurrently': 9, 'onVentilatorCumulative': 10, 
    # 'recovered': 11, 'dateChecked': 12, 'death': 13, 'hospitalized': 14, 
    # 'lastModified': 15, 'total': 16, 'totalTestResults': 17, 'posNeg': 18, 
    # 'deathIncrease': 19, 'hospitalizedIncrease': 20, 'negativeIncrease': 21, 
    # 'positiveIncrease': 22, 'totalTestResultsIncrease': 23, 'hash': 24}
    
    # get states data
    stateData = import_data_file( statesPath )
    
    # {'date': 0, 'state': 1, 'positive': 2, 'negative': 3, 'pending': 4, 
    # 'hospitalizedCurrently': 5, 'hospitalizedCumulative': 6, 'inIcuCurrently': 7, 
    # 'inIcuCumulative': 8, 'onVentilatorCurrently': 9, 'onVentilatorCumulative': 10, 
    # 'recovered': 11, 'dataQualityGrade': 12, 'lastUpdateEt': 13, 'dateModified': 14, 
    # 'checkTimeEt': 15, 'death': 16, 'hospitalized': 17, 'dateChecked': 18, 
    # 'totalTestsViral': 19, 'positiveTestsViral': 20, 'negativeTestsViral': 21, 
    # 'positiveCasesViral': 22, 'deathConfirmed': 23, 'deathProbable': 24, 'fips': 25, 
    # 'positiveIncrease': 26, 'negativeIncrease': 27, 'total': 28, 
    # 'totalTestResults': 29, 'totalTestResultsIncrease': 30, 'posNeg': 31, 
    # 'deathIncrease': 32, 'hospitalizedIncrease': 33, 'hash': 34, 'commercialScore': 35, 
    # 'negativeRegularScore': 36, 'negativeScore': 37, 'positiveScore': 38, 'score': 39, 
    # 'grade': 40}
    
    # reformat dates is US data
    for d in usData[ 1 : ]:
    
        # add hypthens to date
        date = d[ 0 ]
        reformattedDate = f'{date[ : 4 ]}-{date[ 4 : 6 ]}-{date[ 6 : ]}'
        d[ 0 ] = reformattedDate
        
    # reformat dates and states in states data
    for d in stateData:
        
        # add hypthens to date
        date = d[ 0 ]
        reformattedDate = f'{date[ : 4 ]}-{date[ 4 : 6 ]}-{date[ 6 : ]}'
        d[ 0 ] = reformattedDate
        
        # get state from abbreviatin
        d[ 1 ] = state_from_postal_code( d[ 1 ] )
    
    # reduce dataset by criteria
    limitedData = [ ]   # list of lines of data based on state criteria
    
    # evaluate each state criterion
    for state in statesCriteria:
    
        # get the subset of data lines for a specific criterion in each dataset
        subset = [ line for line in stateData if not state or line[ 1 ] == state ]
        limitedData.extend( subset )
        
    # get lists of dates and states
    dates = sorted( list( { fields[ 0 ] for fields in limitedData + usData[ 1 : ] }))
    states = sorted( list( { fields[ 1 ] for fields in limitedData }))
    
    # make states dict { tuple ( str date, str state ) :
    #                   dict { str field names : str counts }}
    statesDct = { ( fields[ 0 ], fields [ 1 ] ) :
                    { 'positives' : fields[ 2 ], 'hospitalized' : fields[ 6 ],
                    'icu' : fields[ 8 ], 'ventilator' : fields[ 10 ],
                    'deaths' : fields[ 16 ] } for fields in limitedData }
                    
    # make US dict { str date : dict { str field names : str counts }}
    usDct = { fields[ 0 ] :
                    { 'positives' : fields[ 2 ], 'hospitalized' : fields[ 6 ],
                    'icu' : fields[ 8 ], 'ventilator' : fields[ 10 ],
                    'deaths' : fields[ 13 ] } for fields in usData }
    
    # list of fields to report
    fields = [ 'positives', 'hospitalized', 'icu', 'ventilator', 'deaths' ]
    
    # format output header lines
    headerLine1 = [ '' ]
    headerLine2 = [ 'date' ]
    
    for i in [ 'USA' ] + statesCriteria:
        headerLine1.extend( [ i ] * len( fields ))
        headerLine1.append( '' ) # spacer
        headerLine2.extend( fields )
        headerLine2.append( '' ) # spacer
        
    output.append( '\t'.join( headerLine1 ))
    output.append( '\t'.join( headerLine2 ))

    # go thru the list of dates                
    for date in dates:
        
        # make list of fields to be joined and appended to output
        outputLine = [ date ]
        
        # get the US data for this date
        fieldsDct = usDct.get( date, { } )
        
        for f in fields:
            outputLine.append( fieldsDct.get( f, '' ) )
            
        outputLine.append( '' ) # spacer
            
       # get state data
        for state in states:
        
            # get the state data for this date
            fieldsDct = statesDct.get( ( date, state ), { } )
        
            for f in fields:
                outputLine.append( fieldsDct.get( f, '' ) )
            
            outputLine.append( '' ) # spacer
        
        # add the line of data to the output    
        output.append( '\t'.join( outputLine ))
        
    print( 'YES' )
    
    return output
    
def parse_census_county_data( countyCriteria, path = 'covid_data/usc_counties_2019.txt' ):
    """ This function reduces the full US Census counties dataset down to a table of
    the population of specified counties. """
    # ARGUMENT countyCriteria -> list of tuples ( str State, str County ) to limit dataset
    # ARGUMENT path -> str path of US Census counties data

    # RETURN
    output = [ 'state\tcounty\tpopulation' ] # list of str tab-delimited data
    
    print( 'Parsing county data from the USA Census...', end = ' ' )
    
    # import the complete dataset from the file
    data = import_data_file( path )
    
    # field indices of the US Census county data
    # {'SUMLEV': 0, 'REGION': 1, 'DIVISION': 2, 'STATE': 3, 'COUNTY': 4, 'STNAME': 5,
    #   'CTYNAME': 6, 'CENSUS2010POP': 7, 'ESTIMATESBASE2010': 8, 'POPESTIMATE2010': 9,
    #   'POPESTIMATE2011': 10, 'POPESTIMATE2012': 11, 'POPESTIMATE2013': 12,
    #   'POPESTIMATE2014': 13, 'POPESTIMATE2015': 14, 'POPESTIMATE2016': 15,
    #   'POPESTIMATE2017': 16, 'POPESTIMATE2018': 17, 'POPESTIMATE2019': 18, ...
    
    # reduce dataset by criteria
    limitedData = [ ]   # list of lines of data based on county criteria
    
    # evaluate each ( state, county) criterion
    for ( state, county ) in countyCriteria:
    
        # get the subset of data lines for a specific criterion
        subset = [ line for line in data
                    if ( not state or line[ 5 ] == state )
                    and ( not county or line[ 6 ].replace( ' County', '' ) == county ) ]
        
        # add the subet to the set of limited data
        limitedData.extend( subset )
    
    # go thru each line of the limited dataset    
    for line in limitedData:
    
        # parse specific fields
        state = line[ 5 ]
        county = line[ 6 ].replace( ' County', '' )
        population = line[ 18 ]
        
        # append fields to the output file
        output.append( '\t'.join( [ state, county, population ] ))
        
    print( 'YES' )
        
    return output
    
def parse_nyt_county_data_by_date( countyCriteria, path = 'covid_data/nyt_us_counties.txt' ):
    """ This function reduces the full NY Times counties dataset down to a table of
    results by date. """
    # ARGUMENT countyCriteria -> list of tuples ( str State, str County ) to limit dataset
    # ARGUMENT path -> str path of NY Times counties data

    # RETURN
    output = [ ]    # list of str tab-delimited data
    
    print( 'Parsing county data from the NY Times...', end = ' ' )

    # import the complete dataset from the file
    data = import_data_file( path )    
    
    # {'date': 0, 'county': 1, 'state': 2, 'fips': 3, 'cases': 4, 'deaths': 5}
    
    # reduced dataset by criteria
    limitedData = [ ]   # list of lines of data based on county criteria
    
    # evaluate each ( state, county) criterion
    for ( state, county ) in countyCriteria:
    
        # get the subset of data lines for a specific criterion
        subset = [ line for line in data
                    if ( not state or line[ 2 ] == state )
                    and ( not county or line[ 1 ] == county ) ]
                    
        # add the subet to the set of limited data
        limitedData.extend( subset )
    
    # get the sets of dates and counties as sorted lists
    # counties are represented as tuples ( State, County )    
    dates = sorted( list( { fields[ 0 ] for fields in limitedData } ))
    counties = sorted( list( { ( fields[ 2 ], fields[ 1 ] ) for fields in limitedData }))
    
    # create cases and deaths dictionaries
    # dict { tuple ( str date, tuple ( state, county )) : str case or death count }
    casesDct = { ( fields[ 0 ], ( fields[ 2 ], fields[ 1 ] ) ) : fields[ 4 ]
                for fields in limitedData }
    deathsDct = { ( fields[ 0 ], ( fields[ 2 ], fields[ 1 ] ) ) : fields[ 5 ]
                for fields in limitedData }
                
    def encapsulte_output_cycles( title, dct ):
        """ This subroutine encapsulates the code for making the cases and deaths
        tables in succession. """
        # ARGUMENT title -> str table title
        # ARGUMENT dct -> dct for either casesDct or deathsDct
        
        # RETURN nothing
    
        # state and county headers for case         
        output.append( f'{title.upper( )}\t' + '\t'.join( [ county[ 0 ] for county in counties ] ))
        output.append( 'date\t' + '\t'.join( [ county[ 1 ] for county in counties ] ))
    
        # go thru list of dates
        for date in dates:
        
            # make list of fields to be joined and appended to output
            outputFields = [ date ]
        
            # append data for each county
            for county in counties:
                outputFields.append( str( dct.get( ( date, county ), 0 ) ) )
            
            # append the joined fields to the output list    
            output.append( '\t'.join( outputFields ))
            
        output.append( '' ) # spacer
            
        return
    
    # make the two tables    
    encapsulte_output_cycles( 'CASES', casesDct )
    encapsulte_output_cycles( 'DEATHS', deathsDct )
    
    print( 'YES' )
    
    return output
    
def state_from_postal_code( abbrev ):
    """ This function provides a full state name from a two-letter abbreviation. If 
    the abbreviation isn't in the dictionary, the abbreviation itself is return. """
    # ARGUMENT abbrev -> str 2-letter state abbreviation
    
    # RETURN
    state = ''  # str state name

    statesAbbrevDct = { 'AL' : 'Alabama', 'AK' : 'Alaska', 'AZ' : 'Arizona', 
                    'AR' : 'Arkansas', 'CA' : 'California', 'CO' : 'Colorado', 
                    'CT' : 'Connecticut', 'DE' : 'Delaware', 'FL' : 'Florida', 
                    'GA' : 'Georgia', 'HI' : 'Hawaii', 'ID' : 'Idaho', 'IL' : 'Illinois', 
                    'IN' : 'Indiana', 'IA' : 'Iowa', 'KS' : 'Kansas', 'KY' : 'Kentucky', 
                    'LA' : 'Louisiana', 'ME' : 'Maine', 'MD' : 'Maryland', 
                    'MA' : 'Massachusetts', 'MI' : 'Michigan', 'MN' : 'Minnesota', 
                    'MS' : 'Mississippi', 'MO' : 'Missouri', 'MT' : 'Montana', 
                    'NE' : 'Nebraska', 'NV' : 'Nevada', 'NH' : 'New Hampshire', 
                    'NJ' : 'New Jersey', 'NM' : 'New Mexico', 'NY' : 'New York', 
                    'NC' : 'North Carolina', 'ND' : 'North Dakota', 'OH' : 'Ohio', 
                    'OK' : 'Oklahoma', 'OR' : 'Oregon', 'PA' : 'Pennsylvania', 
                    'RI' : 'Rhode Island', 'SC' : 'South Carolina', 'SD' : 'South Dakota', 
                    'TN' : 'Tennessee', 'TX' : 'Texas', 'UT' : 'Utah', 'VT' : 'Vermont', 
                    'VA' : 'Virginia', 'WA' : 'Washington', 'WV' : 'West Virginia', 
                    'WI' : 'Wisconsin', 'WY' : 'Wyoming', 'GU' : 'Guam', 
                    'PR' : 'Puerto Rico', 'MP' : 'Northern Mariana Islands', 
                    'AS' : 'American Samoa', 'VI' : 'Virgin Islands',
                    'DC' : 'District of Columbia' }
                    
    state = statesAbbrevDct.get( abbrev.upper( ), f'{abbrev}**' )

    return state

def export_lines_to_file( lines, path ):
    """ This function writes lines of text to a file. """
    # ARGUMENT lines -> list of str text
    # ARGUMENT path -> str path and file name to write
    
    # RETURN nothing

    with open( path, 'w' ) as fileOut:
        for line in lines:
            print( line, file = fileOut )

    return
    
def import_data_file( path ):
    """ This function reads lines from a text file and parses the fields in
    each tab-delimited line. """
    # ARGUMENT path -> str path and file name of file to read
    
    # RETURN
    lines = [ ] # list of lists of str fields

    with open( path ) as fileIn:
        lines = [ line.split( '\t' ) for line in fileIn.read( ).split( '\n' )
                    if line ]
                    
    return lines
        
    
if __name__ == '__main__':
    main( )