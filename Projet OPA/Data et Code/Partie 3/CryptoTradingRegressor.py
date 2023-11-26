import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sqlalchemy import create_engine

# Connexion à la base de données
db_url = "à remplir par khalil"
engine = create_engine(db_url)

# Extraction des données
query = """
SELECT * FROM historical_data ORDER BY time_close;
"""
data = pd.read_sql(query, engine)

# Création de caractéristiques de retard (lag features)
lag_periods = 5
for i in range(1, lag_periods + 1):
    data[f'lag_{i}'] = data['price_close'].shift(i)

# Supprimer les lignes avec des valeurs NaN créées par le décalage
data = data.dropna()

# Définir les caractéristiques et la variable cible
X = data.drop(['price_close', 'time_start', 'time_end', 'time_open', 'time_close'])
y = data['price_close']

# Division en ensembles d'entraînement et de test
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

# Création et entraînement du modèle de régression
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Prédiction et évaluation
predictions = model.predict(X_test)
mse = mean_squared_error(y_test, predictions)
print(f'Mean Squared Error: {mse}')

# Stratégie de trading 
# Acheter si le modèle prédit une augmentation du prix par rapport au dernier prix de clôture connu
# Vendre dans le cas contraire
buy_signals, sell_signals= predictions > X_test['lag_1'], predictions < X_test['lag_1']
