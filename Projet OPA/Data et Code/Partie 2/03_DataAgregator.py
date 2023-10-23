#!/usr/bin/env python3

import requests
from requests import get
from pprint import pprint
import pandas as pd
import logging
from decouple import config
from tqdm import tqdm
from datetime import date, timedelta

import Factory

def GetConfigParameters() :
    """This function get the configuration parameters 
    from the configuration file and return is back as a set 
    of strings 
    
    param : None

    returns : Tuple (str, str, str)
        strCoinAPIKeyIdentifier : Identifier used in the header to connect connect to the API 
        strURLHistoricalData : URL used to retrive the historical data from COIN API
        strHistoricalDataPeriod : Period of historical data
    """

    logging.debug('Entering GetConfigParameters function')

    strCoinAPIKeyIdentifier = config('CoinAPIKeyIdentifier')
    strURLHistoricalData = config('URLHistoricalData')
    strHistoricalDataPeriod = config('URLHistDataPeriod')
    strHistoricalDataDelta = config('HistDateDeltaInDays')

    logging.info(f'Data retrived from the config file are: CoinAPIKeyIdentifier: {strCoinAPIKeyIdentifier} and strURLHistoricalData : {strURLHistoricalData} and strHistoricalDataPeriod : {strHistoricalDataPeriod} and time delta {strHistoricalDataDelta}')

    logging.debug('Exiting GetConfigParameters function')

    return (strCoinAPIKeyIdentifier, strURLHistoricalData, strHistoricalDataPeriod, strHistoricalDataDelta)


def GetHistoryStartingDate(intDateDelta:int):
    """This function will format and get the date of the starting of the historical data
    the time period between the date of today and the date of the historical data starting

    param :  
        intDateDelta : int
            date delta between the date of today and the starting date of the historical data
    returns  str:
            connection object to the database
    """
    logging.debug('Entering GetHistoryStartingDate function')

    dtStartingDate = date.today() - timedelta(days=intDateDelta)
    strStartingDate = dtStartingDate.strftime("%Y-%m-%dT%H:%M:%S")

    return strStartingDate
    logging.debug('Exiting GetHistoryStartingDate function')



def GetTheHeaderWIthRightAPIKeyFromCryptoSymbol(strCryptoSymbol:str):
    """
    This function allow to build the header used to query the COIN API end point

    param : strCryptoSymbol:str
            symbol of the crypto to be checked

    returns : dict
               header used to query the API

    """

    logging.debug("Entering GetTheHeaderWIthRightAPIKeyFromCryptoSymbol function")


    if (strCryptoSymbol == "BTC"):
        strAPIKey:str = config("BTC_API_HIST")
    elif (strCryptoSymbol == "ETH"):
        strAPIKey:str = config("ETH_API_HIST")
    elif (strCryptoSymbol == "LTC"):
        strAPIKey:str = config("LTC_API_HIST")
    else :
        strAPIKey:str = ""
    
    return strAPIKey

    logging.debug("Exiting GetTheHeaderWIthRightAPIKeyFromCryptoSymbol function")


def GetHistoricalDataFromCoinAPIAndSaveItToDatabase(strCoinAPIKeyIdentifier:str, strURLHistoricalData:str, strHistoricalDataPeriod:str, strHistoricalDateDelta:str , ltCryptocurrencyFetchedData:str, Conn):
    """This function gets the real time data from the coin API using the parameters retirved  
     from the config file

    param :  

        strCoinAPIKeyIdentifier : str
            Identifier used in the header to connect connect to the API 
        strURLHistoricalData : str
            URL used to retrive the historical data from COIN API
        strHistoricalDataPeriod:str 
            Time period between historical records
        strHistoricalDataDelta:str 
            date delta between the date of today and the starting date of the historical data
        ltCryptocurrencyFetchedData:list
            list of cryptocurrencies fetched from the croptocurrency database
        Conn:
            connection object to the database
    """

    logging.debug('Entering GetHistoricalDataFromCoinAPIAndSaveItToDatabase function')

    for strFetchedDataItem in tqdm(ltCryptocurrencyFetchedData):

        strIdOftheCrypto = strFetchedDataItem[0]
        strCryptoname =  strFetchedDataItem[1]
        strCryptoSymbol =  strFetchedDataItem[2]

        logging.info(f"Checking if the cryptoacronym {strCryptoSymbol} is among the list to be updated")

        blCryptoSymbolToBeUpated = Factory.CheckIfCryptoSymbolIsAmongListToBeUpdated(strCryptoSymbol, strListKey="ltListOfSymbolToBeUpated") # to be updated


        if blCryptoSymbolToBeUpated:
            logging.info(f"Getting the starting date of the historical data feed")
            strHistoryDataStartingDate = GetHistoryStartingDate(int(strHistoricalDataDelta))

            logging.info(f"Building the query uusing the fetch data from the COin API endpoint")
            strURLToFetchHistorical = strURLHistoricalData.format(strCryptoSymbol, strHistoricalDataPeriod, strHistoryDataStartingDate)

            logging.info(f'Preparing the headers of the requesting using the information retrived from config file')
            strAPIKey:str = GetTheHeaderWIthRightAPIKeyFromCryptoSymbol(strCryptoSymbol)
            headers = {strCoinAPIKeyIdentifier : strAPIKey}

            logging.info(f'requesting historical data from coin API for coin : {strCryptoname} with sympbol : {strCryptoSymbol}')
            response = requests.get(strURLToFetchHistorical, headers=headers)

            if (response.status_code == 200):
                
                logging.info(f'Adding historical data for coin : {strCryptoname} with sympbol : {strCryptoSymbol} fetched from coin api with response')

                for jnResponseItem in tqdm(response.json()):
                    logging.info(f'Extracting historical data item of crypto {strCryptoname} from Coin API response')
                    strTimePeriodStart, strTimePeriodEnd, strTimeOpen, strTimeClose, strPriceOpen, strPriceHigh, strPriceLow , strPriceClose, strVolumeTraded, strTradesCount = ExtractDataFromCoinAPIResponseItem(jnResponseItem)

                    logging.info(f'Saving historical item entry of crypto {strCryptoname} with starting time {strTimePeriodStart} and end time {strTimePeriodEnd} to historical data table')   
                    SaveCryptoRealTimeDataToRealTimeDataTable(strIdOftheCrypto, strTimePeriodStart, strTimePeriodEnd, strTimeOpen, strTimeClose, strPriceOpen, strPriceHigh, strPriceLow , strPriceClose, strVolumeTraded, strTradesCount, strCryptoSymbol ,Conn)

            else:
                logging.error(f"Request for historical data from Coin API endpoint failed for coin : {strCryptoname} with sympbol : {strCryptoSymbol} with message error {response.text}")

        else:
            logging.debug(f'Crypto symbol {strCryptoSymbol} is not among the list to be updated')


def SaveCryptoRealTimeDataToRealTimeDataTable(strIdOftheCrypto, strTimePeriodStart, strTimePeriodEnd, strTimeOpen, strTimeClose, strPriceOpen, strPriceHigh, strPriceLow , strPriceClose, strVolumeTraded, strTradesCount, strCryptoSymbol, Conn, strQueryName:str="StrAddTodateToHistoricalTableQuery"):
    """
    This function save the historical data item to the historical_data table 
    
    param : 
        strTimePeriodStart : Time period start 
        strTimePeriodEnd : Time period end 
        strTimeOpen : Actual time of market opening
        strTimeClose : Actual time of market closing
        strPriceOpen : Price for openig 
        strPriceHigh : Highest price achieved during the period  
        strPriceLow : Lowest price achieved during the period  
        strPriceClose : Price for closing 
        strVolumeTraded : Volume of crypto traded
        strTradesCount : number of trades
        strCryptoSymbol :  Symbol of the crypto
        Conn : connection object to the database
        strQueryName : name of the query to be used to add data to the historical data table

    """
    logging.debug('Entering ExtractDataFromCoinAPIResponseItem function')

    logging.info(f"Building query to add the historical data item into  for crypto {strCryptoSymbol}")
    strBuiltQueryToAdd:str = config(strQueryName).format(strIdOftheCrypto, strTimePeriodStart, strTimePeriodEnd, strTimeOpen, strTimeClose, strPriceOpen, strPriceHigh, strPriceLow , strPriceClose, strVolumeTraded, strTradesCount, Conn)

    logging.info(f"Sending historical data for crypto {strCryptoSymbol} to the historical_data table")
    cursor = Conn.cursor()
    cursor.execute(strBuiltQueryToAdd)
    Conn.commit()
    cursor.close()

    logging.debug('Existing ExtractDataFromCoinAPIResponseItem function')

def ExtractDataFromCoinAPIResponseItem(dcResponseItem:dict):
    """
    This function allows to retrive data from COIN API Item response 
    
    param :  

        dcResponseItem:dict
            Response obtained from COIn API endpoint

    returns : Tuple (str, str, str, str, str, str, str, str, str, str)
        strTimePeriodStart : Time period start 
        strTimePeriodEnd : Time period end 
        strTimeOpen : Actual time of market opening
        strTimeClose : Actual time of market closing
        strPriceOpen : Price for openig 
        strPriceHigh : Highest price achieved during the period  
        strPriceLow : Lowest price achieved during the period  
        strPriceClose : Price for closing 
        strVolumeTraded : Volume of crypto traded
        strTradesCount : number of trades
    """
    logging.debug('Entering ExtractDataFromCoinAPIResponseItem function')
    
    strTimePeriodStart = dcResponseItem['time_period_start']
    strTimePeriodEnd = dcResponseItem['time_period_end']
    strTimeOpen = dcResponseItem['time_open']
    strTimeClose = dcResponseItem['time_close']
    strPriceOpen = dcResponseItem['price_open']
    strPriceHigh = dcResponseItem['price_high']
    strPriceLow = dcResponseItem['price_low'] 
    strPriceClose = dcResponseItem['price_close']
    strVolumeTraded = dcResponseItem['volume_traded'] 
    strTradesCount = dcResponseItem['trades_count']

    logging.debug('Existing ExtractDataFromCoinAPIResponseItem function')

    return (strTimePeriodStart, strTimePeriodEnd, strTimeOpen, strTimeClose, strPriceOpen, strPriceHigh, strPriceLow , strPriceClose, strVolumeTraded, strTradesCount)


if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR) 
    
    logging.info('Getting parameters information for historical data from config file using GetConfigParameters function')
    strCoinAPIKeyIdentifier, strURLHistoricalData, strHistoricalDataPeriod, strHistoricalDataDelta = GetConfigParameters()

    logging.info('Getting connection object from factory ConnectToDatabase function')
    Conn = Factory.ConnectToDatabase()

    logging.info('Getting connection object from GetDataFromCryptocurrencyDatabase function')
    ltCryptocurrencyFetchedData = Factory.GetDataFromCryptocurrencyDatabase(Conn)
    
    # requesting real time data from COIN API 
    logging.info('requesting historical data from COIN API using GetRealTimeDataFromCoinAPI function')
    GetHistoricalDataFromCoinAPIAndSaveItToDatabase(strCoinAPIKeyIdentifier, strURLHistoricalData, strHistoricalDataPeriod, strHistoricalDataDelta, ltCryptocurrencyFetchedData, Conn)


