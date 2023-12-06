#!/usr/bin/env python3
from fastapi import FastAPI

from sqlalchemy import create_engine
import pandas as pd

import TrainBitCoinModel
import TrainEthereumModel
import TrainLiteCoinModel

import PredictBitCoinTrade
import PredictEthereumTrade
import PredictLiteCoinTrade

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import pickle
from decouple import config
import logging
from logging.handlers import RotatingFileHandler
import Factory
import os

api = FastAPI()

def GetTheLastTrainModelPerformance(strIdOftheCrypto:str, Conn):
    logging.debug('Entering GetTheLastTrainModelPerformance function')
    
    logging.info(f"Building query to put date into model update database for crypto {strIdOftheCrypto}")
    strBuiltQueryToAdd:str = config("strQueryModelUpdateLastTraining").format(strIdOftheCrypto)
    logging.info('Fetching data from model update table')
    cursor = Conn.cursor()
    cursor.execute(strBuiltQueryToAdd)
    ltFetchedData = cursor.fetchone()

    if(ltFetchedData[3] == '1'):
        blModelTrained = True
    else:
        blModelTrained = False

    if(ltFetchedData[4] == '1'):
        blModelUpdated = True
    else:
        blModelUpdated = False

    ftOldModelPerformance = ltFetchedData[5]
    ftNewModelPerformance = ltFetchedData[6]

    cursor.close()
    logging.debug('Exiting GetTheLastTrainModelPerformance function')

    return (blModelTrained, blModelUpdated, ftOldModelPerformance, ftNewModelPerformance)


def getTheLastTradeInformation(strIdOftheCrypto:str, Conn):
    logging.debug('Entering getTheLastTradeInformation function')
 
    logging.info(f"Building query to get trade data for crypto id {strIdOftheCrypto}")
    strBuiltQueryToAdd:str = config("strQueryModelPredictionLastTrade").format(strIdOftheCrypto)

    logging.info('Fetching data from model prediction table')
    cursor = Conn.cursor()
    cursor.execute(strBuiltQueryToAdd)
    ltFetchedData = cursor.fetchone()

    if(ltFetchedData[3] == '1'):
        blBuySignal = True
    else:
        blBuySignal = False

    if(ltFetchedData[4] == '1'):
        blSellSignal = True
    else:
        blSellSignal = False

    ftLastCryptoValue = ltFetchedData[5]
    ftPredictedFuturePrice = ltFetchedData[6]
    
    logging.debug('Exiting getTheLastTradeInformation function')

    return (blBuySignal, blSellSignal, ftLastCryptoValue, ftPredictedFuturePrice)


def predictBitcoinPrice():
    logging.debug('Entering predictBitcoinPrice function')

    logging.info('Pridicting BitCoin Price')
    PredictBitCoinTrade.main()

    logging.info('Getting the new BitCoin model performance')
    logging.info('Getting connection object from factory ConnectToDatabase function')
    Conn = Factory.ConnectToDatabase()

    blBuySignal, blSellSignal, ftLastCryptoValue, ftPredictedFuturePrice = getTheLastTradeInformation("1", Conn)


    logging.debug('Exiting predictBitcoinPrice function')
    return (blBuySignal, blSellSignal, ftLastCryptoValue, ftPredictedFuturePrice)

def predictLitecoinPrice():
    logging.debug('Entering predictLitecoinPrice function')

    logging.info('Pridicting Litecoin Price')
    PredictLiteCoinTrade.main()

    logging.info('Getting the new Litecoin model performance')
    logging.info('Getting connection object from factory ConnectToDatabase function')
    Conn = Factory.ConnectToDatabase()

    blBuySignal, blSellSignal, ftLastCryptoValue, ftPredictedFuturePrice = getTheLastTradeInformation("3", Conn)

    logging.debug('Exiting predictLitecoinPrice function')
    return (blBuySignal, blSellSignal, ftLastCryptoValue, ftPredictedFuturePrice)

def predictEthereumPrice():
    logging.debug('Entering predictEthereumPrice function')

    logging.info('Pridicting Ethereum Price')
    PredictEthereumTrade.main()

    logging.info('Getting the new Ethereum model performance')
    logging.info('Getting connection object from factory ConnectToDatabase function')
    Conn = Factory.ConnectToDatabase()

    blBuySignal, blSellSignal, ftLastCryptoValue, ftPredictedFuturePrice = getTheLastTradeInformation("98", Conn)

    logging.debug('Exiting predictEthereumPrice function')
    return (blBuySignal, blSellSignal, ftLastCryptoValue, ftPredictedFuturePrice)




def trainBitCoinModel():
    logging.debug('Entering TrainBitCoinModel function')

    logging.info('Training BitCoin Model')
    TrainBitCoinModel.main()

    logging.info('Getting the new BitCoin model performance')
    logging.info('Getting connection object from factory ConnectToDatabase function')
    Conn = Factory.ConnectToDatabase()

    blModelTrained, blModelUpdated, ftOldModelPerformance, ftNewModelPerformance = GetTheLastTrainModelPerformance("1", Conn)

    logging.debug('Exiting TrainBitCoinModel function')

    return (blModelTrained, blModelUpdated, ftOldModelPerformance, ftNewModelPerformance)


def trainLiteCoinModel():
    logging.debug('Entering trainLiteCoinModel function')

    logging.info('Training LiteCoin Model')
    TrainLiteCoinModel.main()

    logging.info('Getting the new LiteCoin model performance')
    logging.info('Getting connection object from factory ConnectToDatabase function')
    Conn = Factory.ConnectToDatabase()

    blModelTrained, blModelUpdated, ftOldModelPerformance, ftNewModelPerformance = GetTheLastTrainModelPerformance("3", Conn)

    logging.debug('Exiting trainLiteCoinModel function')

    return (blModelTrained, blModelUpdated, ftOldModelPerformance, ftNewModelPerformance)


def trainEthereumModel():
    logging.debug('Entering trainEthereumModel function')

    logging.info('Training Ethereum Model')
    TrainEthereumModel.main()

    logging.info('Getting the new Ethereum model performance')
    logging.info('Getting connection object from factory ConnectToDatabase function')
    Conn = Factory.ConnectToDatabase()

    blModelTrained, blModelUpdated, ftOldModelPerformance, ftNewModelPerformance = GetTheLastTrainModelPerformance("98", Conn)

    logging.debug('Exiting trainEthereumModel function')

    return (blModelTrained, blModelUpdated, ftOldModelPerformance, ftNewModelPerformance)


@api.get('/TrainModel/{Cryptoid:str}')
def TrainModel(Cryptoid):

    if(Cryptoid == "1"):
        blModelTrained, blModelUpdated, ftOldModelPerformance, ftNewModelPerformance = trainBitCoinModel()
        return {
            "Crypto": "BitCoin",
            "Trained new Model": blModelTrained,
            "Updated Model ": blModelUpdated,
            "Old Model Performance" : ftOldModelPerformance,
            "New Model Performance" : ftNewModelPerformance
        }
    elif(Cryptoid == "3"):
        blModelTrained, blModelUpdated, ftOldModelPerformance, ftNewModelPerformance = trainLiteCoinModel()
        return {
            "Crypto": "LiteCoin",
            "Trained new Model": blModelTrained,
            "Updated Model ": blModelUpdated,
            "Old Model Performance" : ftOldModelPerformance,
            "New Model Performance" : ftNewModelPerformance
        }
    elif(Cryptoid == "98"):
        blModelTrained, blModelUpdated, ftOldModelPerformance, ftNewModelPerformance = trainEthereumModel()
        return {
            "Crypto": "Ethereum",
            "Trained new Model": blModelTrained,
            "Updated Model ": blModelUpdated,
            "Old Model Performance" : ftOldModelPerformance,
            "New Model Performance" : ftNewModelPerformance
        }
    
    else:
        logging.error("Unknow crypto id {} to train Model".format(Cryptoid))
        return {"Error": "Unknow crypto id {} to train Model".format(Cryptoid)}
        

@api.get('/PredictTrade/{Cryptoid:str}')
def TrainModel(Cryptoid):

    if(Cryptoid == "1"):
        blBuySignal, blSellSignal, ftLastCryptoValue, ftPredictedFuturePrice =  predictBitcoinPrice()
        
        return {
            "Crypto": "BitCoin",
            "Buy Signal": blBuySignal,
            "Sell Signal": blSellSignal,
            "Last Recorded Price" : ftLastCryptoValue,
            "Predicted Future Price" : ftPredictedFuturePrice
        }
    elif(Cryptoid == "3"):
        blBuySignal, blSellSignal, ftLastCryptoValue, ftPredictedFuturePrice =  predictLitecoinPrice()
        
        return {
            "Crypto": "Litecoin",
            "Buy Signal": blBuySignal,
            "Sell Signal": blSellSignal,
            "Last Recorded Price" : ftLastCryptoValue,
            "Predicted Future Price" : ftPredictedFuturePrice
        }

    elif(Cryptoid == "98"):
        blBuySignal, blSellSignal, ftLastCryptoValue, ftPredictedFuturePrice =  predictEthereumPrice()
        
        return {
            "Crypto": "Ethereum",
            "Buy Signal": blBuySignal,
            "Sell Signal": blSellSignal,
            "Last Recorded Price" : ftLastCryptoValue,
            "Predicted Future Price" : ftPredictedFuturePrice
        }

    else:
        logging.error("Unknow crypto id {} to Predict trade".format(Cryptoid))
        return {"Error": "Unknow crypto id {} to Predict trade".format(Cryptoid)}