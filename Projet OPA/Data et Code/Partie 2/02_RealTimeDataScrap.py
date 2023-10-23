#!/usr/bin/env python3

import requests
from requests import get
from pprint import pprint
import pandas as pd
import logging
from decouple import config
import Factory
import psycopg2
from tqdm import tqdm
from dateutil import parser


def GetConfigParameters() :
    """This function get the configuration parameters 
    from the configuration file and return is back as a set 
    of strings 
    
    param : None

    returns : Tuple (str, str)
        strCoinAPIKeyIdentifier : Identifier used in the header to connect connect to the API 
        strURLRealTimeData : URL used to retrive the real time data from COIN API
    """

    logging.debug('Entering GetConfigParameters function')

    strCoinAPIKeyIdentifier = config("CoinAPIKeyIdentifier")
    strURLRealTimeData = config("URLRealTimeData")

    logging.info(f'Data retrived from the config file are: CoinAPIKeyIdentifier: {strCoinAPIKeyIdentifier} and URLRealTimeData : {strURLRealTimeData}')

    logging.debug('Exiting GetConfigParameters function')

    return (strCoinAPIKeyIdentifier, strURLRealTimeData)

def CheckIfCryptoSymbolIsAmongListToBeUpdated(strCryptoSymbol:str , strListKey:str="ltListOfSymbolToBeUpated" ) :
    """
    This function allows to check if the symbol of the crypto is among the list to be updated

    param : strCryptoSymbol:str
            symbol of the crypto to be checked



    returns : bool
                True if the symbol is among the list of acrynym to be updated
                False if the symbol is not among the list of acrynym to be updated
    
    """

    logging.debug('Entering CheckIfCryptoSymbolIsAmongListToBeUpdated function')

    ltListOfSymbolToBeUpdated:list = config(strListKey)

    if strCryptoSymbol in ltListOfSymbolToBeUpdated:
        logging.debug(f"The crypto symbol {strCryptoSymbol} is among the list of crypto to be updated")
        logging.debug('Exiting CheckIfCryptoSymbolIsAmongListToBeUpdated function')
        return True
    else:
        logging.debug(f"The crypto symbol {strCryptoSymbol} is not among the list of crypto to be updated")
        logging.debug('Exiting CheckIfCryptoSymbolIsAmongListToBeUpdated function')
        return False


def SaveCryptoRealTimeDataToRealTimeDataTable(strIdOftheCrypto:str,strQuoteTimestamp:str,strAssetIdRate:str, strCryptoSymbol:str , Conn , strQueryName:str="StrAddTodateToeRealTimeTableQuery"):
    """
    This function allows to save the real time data fetched from 

    param : strCryptoSymbol:str
            symbol of the crypto to be checked

    returns : int
               index of the crypto in the crypto table
    
    """
    logging.debug("Entering SaveCryptoRealTimeDataToRealTimeDataTable function")

    logging.info(f"Building query to put date into real time databse for crypto {strCryptoSymbol}")
    strBuiltQueryToAdd:str = config(strQueryName).format(strIdOftheCrypto,strQuoteTimestamp, strAssetIdRate)
    
    logging.info(f"Sending data for crypto {strCryptoSymbol} to the realtime database")
    cursor = Conn.cursor()
    cursor.execute(strBuiltQueryToAdd)
    Conn.commit()
    cursor.close()

    logging.debug("Exiting SaveCryptoRealTimeDataToRealTimeDataTable function")


def GetTheHeaderWIthRightAPIKeyFromCryptoSymbol(strCryptoSymbol:str):
    """
    This function allow to build the header used to query the COIn API end point

    param : strCryptoSymbol:str
            symbol of the crypto to be checked

    returns : dict
               header used to query the API

    """

    logging.debug("Entering GetTheHeaderWIthRightAPIKeyFromCryptoSymbol function")


    if (strCryptoSymbol == "BTC"):
        strAPIKey:str = config("BTC_API_KEY")
    elif (strCryptoSymbol == "ETH"):
        strAPIKey:str = config("ETH_API_KEY")
    elif (strCryptoSymbol == "LTC"):
        strAPIKey:str = config("LTC_API_KEY")
    else :
        strAPIKey:str = ""
    
    return strAPIKey

    logging.debug("Exiting GetTheHeaderWIthRightAPIKeyFromCryptoSymbol function")


def GetRealTimeDataFromCoinAPIAndSaveItToDatabase(strCoinAPIKeyIdentifier:str, strURLRealTimeData:str, ltCryptocurrencyFetchedData:list, Conn):
    """This function gets the real time data from the coin API using the parameters retirved  
     from the config file

    param :  

        strAPIKey:str 
            API key used to connect to the COIN API
        strCoinAPIKeyIdentifier : str
            Identifier used in the header to connect connect to the API 
        strURLRealTimeData : str
            URL used to retrive the real time data from COIN API
        ltCryptocurrencyFetchedData:list
            list of cryptocurrencies fetched from the croptocurrency database
        Conn:
            connection object to the database
    """

    logging.debug('Entering GetRealTimeDataFromCoinAPI function')

    for strFetchedDataItem in tqdm(ltCryptocurrencyFetchedData):

        strIdOftheCrypto = strFetchedDataItem[0]
        strCryptoname =  strFetchedDataItem[1]
        strCryptoSymbol =  strFetchedDataItem[2]

        logging.info(f"Checking if the cryptoacronym {strCryptoSymbol} is among the list to be updated")

        blCryptoSymbolToBeUpated = Factory.CheckIfCryptoSymbolIsAmongListToBeUpdated(strCryptoSymbol)


        if blCryptoSymbolToBeUpated:

            logging.info(f'Constructing real time data URL for COIN API for coin : {strCryptoname} with sympbol : {strCryptoSymbol}')
            strURLCryptoFetchingData = strURLRealTimeData.format(strCryptoSymbol)

            logging.info(f'Preparing the headers of the requesting using the information retrived from config file')
            strAPIKey:str = GetTheHeaderWIthRightAPIKeyFromCryptoSymbol(strCryptoSymbol)
            
            headers = {strCoinAPIKeyIdentifier : strAPIKey}

            logging.info(f'requesting real time data from coin API for coin : {strCryptoname} with sympbol : {strCryptoSymbol}')
            response = requests.get(strURLCryptoFetchingData, headers=headers)

            if (response.status_code == 200):

                logging.info(f'Data for real time or coin : {strCryptoname} with sympbol : {strCryptoSymbol} fetched from coin api with response {response.text}')
                
                logging.info(f'Extracting data from Coin API endpoint response')
                strQuoteTimestamp, strAssetIdBase, strAssetIdQuote, strAssetIdRate = ExtractDataFromCoinAPIResponse(response.json())

                logging.info(f'Saving the entry of crypto {strAssetIdBase} with rate {strAssetIdRate} and the time {strQuoteTimestamp} to real time table')    
                SaveCryptoRealTimeDataToRealTimeDataTable(strIdOftheCrypto, strQuoteTimestamp, strAssetIdRate, strCryptoSymbol, Conn)

            else:
                logging.error(f"Request for Coin API endpoint failed for coin : {strCryptoname} with sympbol : {strCryptoSymbol} with message error {response.text}")

            logging.info(f'Sending real time date request to COIN API using the url : {strURLRealTimeData}')

        else:
            logging.debug(f'Crypto symbol {strCryptoSymbol} is not among the list to be updated')


    logging.debug('Existing GetRealTimeDataFromCoinAPI function')

    #return response

def ExtractDataFromCoinAPIResponse(dtCoinAPIResponse:dict):
    """
    This function allows to retrive data from COIn AAPI response 
    
    param :  

        dtCoinAPIResponse:dict
            Response obtained from COIn API endpoint

    returns : Tuple (str, str, str, str)
        strAssetIdBase :  Symbol of the cryptocurrency
        strAssetIdQuote : Currency from which the crytocurrency is being quoted suh as EUR or USD
        strQuoteTimestamp : Datetime of the cryptocurrency quote in string format
        strAssetIdRate : Rete of the cryptocurrency in datetime of the 
    """
    strAssetIdBase = dtCoinAPIResponse["asset_id_base"]
    strAssetIdQuote = dtCoinAPIResponse["asset_id_quote"]
    strAssetIdRate = dtCoinAPIResponse["rate"]
    strQuoteTimestamp = parser.parse(dtCoinAPIResponse['time']).strftime('%Y-%m-%d %H:%M:%S.%f')  

    return (strQuoteTimestamp, strAssetIdBase, strAssetIdQuote, strAssetIdRate)



if __name__ == "__main__":
    
    logging.basicConfig(level=logging.FATAL) 
    # getting paramters from config file

    logging.info('Getting parameters information from config file using GetConfigParameters function')
    strCoinAPIKeyIdentifier, strURLRealTimeData = GetConfigParameters()

    logging.info('Getting connection object from factory ConnectToDatabase function')
    Conn = Factory.ConnectToDatabase()

    logging.info('Getting connection object from GetDataFromCryptocurrencyDatabase function')
    ltCryptocurrencyFetchedData = Factory.GetDataFromCryptocurrencyDatabase(Conn)
    
    # requesting real time data from COIN API 
    logging.info('requesting real time data from COIN API using GetRealTimeDataFromCoinAPI function')

    GetRealTimeDataFromCoinAPIAndSaveItToDatabase(strCoinAPIKeyIdentifier, strURLRealTimeData, ltCryptocurrencyFetchedData, Conn)
