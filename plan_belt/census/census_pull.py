from pg_data_etl import Database
from dotenv import load_dotenv
import pandas as pd
import requests
import os 

load_dotenv()

db = Database.from_config("mercer", "omad")

class CensusTable:
    """A simple class to grab census data from whatever vintage you're interested in. 

    Requires an API, here stored in a .env file on your machine. 

    https://api.census.gov/data/2021/acs/acs5/variables.html holds 2021 ACS 5 year variables for tablenames, find others by replacing the year. 

    """
    
    def __init__(self, year, census, subcensus, table, state, county, apikey, tract, table_type ):
        """
        Parameters
        ----------
        year : str 
            Census year you're interested in
        census : str 
            acs, typically
        subcensus: str
            1 year? 3 year? 5 year? example: acs5 = Five year ACS
        table : str
            The table you're interested in from the variables page on Census.gov
        state : str
            FIPS code for the state you're interested in.
        county : str
            County code for the county you're interested in. 
            TODO: test for multiple states/counties in query
        apikey : str
            Register for one on census.gov, store it in a .env file in your project directory.
        tract : str 
            the tract you're interested in. * = all
        table_type : str
            subject table or detailed table

        TODO:
        break up URL so its more modular when using with different tables
        """
        self.year:str = year
        self.subcensus:str = subcensus
        self.census:str = census 
        self.table:str = table
        self.state:str = state
        self.county:str = county
        self.apikey:str = apikey
        self.tract:str = tract
        self.table_type:str = table_type
        self.api_url = f"https://api.census.gov/data/{self.year}/{self.census}/{self.subcensus}{self.table_type}?get=NAME,{self.table}&for=tract:{self.tract}&in=state:{self.state}&in=county:{self.county}&key={self.apikey}"
        self.r = requests.get(self.api_url)
        self.df = pd.DataFrame.from_dict(self.r.json())
        self.df.columns = self.df.iloc[0]
        self.df = self.df[1:]

