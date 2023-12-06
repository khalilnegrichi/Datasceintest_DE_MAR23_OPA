# base image python
FROM python:3.9.18-slim-bullseye

# Define the project workdir
WORKDIR /home/ubuntu/Projet_OPA_Docker/

# create the project structure
ADD requirements.txt /home/ubuntu/Projet_OPA_Docker/requirements.txt
ADD Factory.py /home/ubuntu/Projet_OPA_Docker/Factory.py
ADD PredictBitCoinTrade.py /home/ubuntu/Projet_OPA_Docker/PredictBitCoinTrade.py
ADD PredictEthereumTrade.py /home/ubuntu/Projet_OPA_Docker/PredictEthereumTrade.py
ADD PredictLiteCoinTrade.py /home/ubuntu/Projet_OPA_Docker/PredictLiteCoinTrade.py
ADD TrainBitCoinModel.py /home/ubuntu/Projet_OPA_Docker/TrainBitCoinModel.py
ADD TrainEthereumModel.py /home/ubuntu/Projet_OPA_Docker/TrainEthereumModel.py
ADD TrainLiteCoinModel.py /home/ubuntu/Projet_OPA_Docker/TrainLiteCoinModel.py
ADD FastAPI.py /home/ubuntu/Projet_OPA_Docker/FastAPI.py
ADD .env /home/ubuntu/Projet_OPA_Docker/.env

RUN mkdir /home/ubuntu/Projet_OPA_Docker/models/
RUN mkdir /home/ubuntu/Projet_OPA_Docker/logs/

# ADD BitCoinPredictionModel.sav /home/ubuntu/Projet_OPA_Docker/models/BitCoinPredictionModel.sav
# ADD EthereumPredictionModel.sav /home/ubuntu/Projet_OPA_Docker/models/EthereumPredictionModel.sav
# ADD LitecoinPredictionModel.sav /home/ubuntu/Projet_OPA_Docker/models/LitecoinPredictionModel.sav


# install the project requirement
RUN pip install -r requirements.txt

# Expose the port on which the application will run
EXPOSE 8000

# train trading models
# CMD [ "python", "./TrainBitCoinModel.py"]
# CMD [ "python", "./TrainEthereumModel.py"]
# CMD [ "python", "./TrainLiteCoinModel.py"]

# Run the FastAPI application using uvicorn server
CMD ["uvicorn", "FastAPI:api", "--host", "0.0.0.0", "--port", "8000"]
