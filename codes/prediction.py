import os
import sys

# ============================================================================
# CRITICAL: Configure environment BEFORE importing PySpark
# ============================================================================
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['PYSPARK_SUBMIT_ARGS'] = '--master local[*] pyspark-shell'
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

if 'JAVA_TOOL_OPTIONS' in os.environ:
    del os.environ['JAVA_TOOL_OPTIONS']
if '_JAVA_OPTIONS' in os.environ:
    del os.environ['_JAVA_OPTIONS']

# ============================================================================
# Import libraries
# ============================================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, dayofweek, month
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql import Row
from datetime import datetime, timedelta
from pymongo import MongoClient
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Crée une session Spark avec la bonne configuration MongoDB"""
    spark = SparkSession.builder \
        .appName("SalesPrediction") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:10.4.0") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Session Spark créée")
    return spark

def load_mongo_data(spark):
    """Charge les données depuis MongoDB"""
    logger.info("Chargement des données depuis MongoDB...")
    
    try:
        # Utiliser les options séparées
        df = spark.read \
            .format("mongodb") \
            .option("uri", "mongodb://localhost:27017") \
            .option("database", "Boutique") \
            .option("collection", "orders") \
            .load()
        
        count = df.count()
        logger.info(f"{count:,} commandes chargées")
        return df
        
    except Exception as e:
        logger.error(f"Erreur: {e}")
        return None

def prepare_data(df):
    """Prépare les données pour l'entraînement"""
    logger.info("Préparation des données...")
    
    # Filtrer les commandes livrées
    df_livré = df.filter(
        (col("status") == "livré") |
        (col("status").contains("livré"))
    )
    
    count = df_livré.count()
    logger.info(f"Commandes livrées: {count:,}")
    
    if count < 5:
        logger.warning("Peu de données pour l'entraînement")
        df_livré = df
        logger.info(f"Utilisation de toutes les commandes: {df_livré.count():,}")
    
    # Extraire les features temporelles
    df_features = df_livré \
        .withColumn("day", dayofmonth(col("date"))) \
        .withColumn("weekday", dayofweek(col("date"))) \
        .withColumn("month", month(col("date"))) \
        .select("total_amount", "day", "weekday", "month")
    
    logger.info("Aperçu des données préparées:")
    df_features.show(5)
    
    return df_features

def train_model(df_features, spark):
    """Entraîne le modèle de prédiction"""
    logger.info("Entraînement du modèle...")
    
    try:
        # Vérifier si assez de données
        if df_features.count() < 3:
            logger.error("Pas assez de données pour l'entraînement")
            return None
        
        # Assembler les features
        assembler = VectorAssembler(
            inputCols=["day", "weekday", "month"],
            outputCol="features"
        )
        
        # Modèle de régression linéaire
        lr = LinearRegression(
            featuresCol="features",
            labelCol="total_amount",
            maxIter=100,
            regParam=0.01,
            elasticNetParam=0.8
        )
        
        # Pipeline
        pipeline = Pipeline(stages=[assembler, lr])
        
        # Entraîner le modèle
        model = pipeline.fit(df_features)
        
        logger.info("Modèle entraîné avec succès")
        
        # Retourner le modèle et la session Spark
        return model, spark
        
    except Exception as e:
        logger.error(f"Erreur lors de l'entraînement: {e}")
        return None, None

def make_predictions(model, spark, days_ahead=7):
    """Fait des prédictions pour les jours à venir - CORRIGÉ"""
    logger.info(f"Génération des prédictions pour {days_ahead} jours...")
    
    try:
        # Créer les données pour prédiction
        prediction_data = []
        today = datetime.now()
        
        for i in range(1, days_ahead + 1):
            future_date = today + timedelta(days=i)
            
            # Calculer le jour de la semaine (PySpark: dimanche=1, lundi=2, etc.)
            weekday_pyspark = future_date.weekday() + 2
            if weekday_pyspark > 7:
                weekday_pyspark = 1
            
            row = {
                "day": future_date.day,
                "weekday": weekday_pyspark,
                "month": future_date.month
            }
            prediction_data.append(row)
        
        # Créer DataFrame Spark - CORRECTION ICI
        prediction_df = spark.createDataFrame(prediction_data)
        
        logger.info("Données de prédiction créées:")
        prediction_df.show()
        
        # Faire les prédictions
        predictions = model.transform(prediction_df)
        
        logger.info("Prédictions générées:")
        predictions.select("day", "month", "weekday", "prediction").show()
        
        # Collecter les résultats
        results = []
        prediction_rows = predictions.collect()
        
        for i, row in enumerate(prediction_rows, 1):
            future_date = today + timedelta(days=i)
            
            result = {
                "prediction_id": f"PRED-{future_date.strftime('%Y%m%d')}",
                "predicted_date": future_date.strftime("%Y-%m-%d"),
                "predicted_sales": float(row["prediction"]),
                "day": int(row["day"]),
                "month": int(row["month"]),
                "weekday": ["Dimanche", "Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi"][future_date.weekday()],
                "model": "LinearRegression",
                "features_used": ["day", "weekday", "month"],
                "created_at": datetime.now().isoformat()
            }
            results.append(result)
        
        logger.info(f"{len(results)} prédictions générées")
        return results
        
    except Exception as e:
        logger.error(f"Erreur lors des prédictions: {e}")
        import traceback
        traceback.print_exc()
        return None

def save_to_mongodb(predictions):
    """Sauvegarde les prédictions dans MongoDB"""
    logger.info("Sauvegarde dans MongoDB...")
    
    try:
        client = MongoClient('localhost', 27017)
        db = client['Boutique']
        
        print(f"Connecté à MongoDB")
        print(f"Base de données: {db.name}")
        
        # Vérifier les collections
        collections = db.list_collection_names()
        print(f"Collections disponibles: {collections}")
        
        # Créer ou vider la collection predictions
        if "predictions" in collections:
            db.predictions.delete_many({})
            print("Anciennes prédictions supprimées")
        else:
            db.create_collection("predictions")
            print("Collection 'predictions' créée")
        
        # Insérer les nouvelles prédictions
        if predictions:
            db.predictions.insert_many(predictions)
            print(f"{len(predictions)} prédictions sauvegardées")
            
            # Afficher un résumé
            print("\n" + "="*60)
            print("RESUME DES PREDICTIONS")
            print("="*60)
            
            total = 0
            for pred in predictions:
                date = pred["predicted_date"]
                amount = pred["predicted_sales"]
                total += amount
                print(f"{date}: {amount:,.2f} TND")
            
            print("-" * 40)
            print(f"TOTAL: {total:,.2f} TND")
            print(f"MOYENNE: {total/len(predictions):,.2f} TND")
            print("="*60)
        
        return True
        
    except Exception as e:
        logger.error(f"Erreur MongoDB: {e}")
        return False

def main():
    """Fonction principale"""
    print("\n" + "="*70)
    print("SYSTEME DE PREDICTION DES VENTES - PySpark")
    print("="*70)
    
    # Demander le nombre de jours à prédire
    try:
        days_ahead = int(input("Combien de jours à prédire? (défaut: 7): ") or "7")
    except:
        days_ahead = 7
    
    spark = None
    try:
        # 1. Créer la session Spark
        spark = create_spark_session()
        
        # 2. Charger les données
        df = load_mongo_data(spark)
        if df is None or df.count() == 0:
            print("\nImpossible de charger les données")
            return
        
        print(f"\nDonnées chargées: {df.count():,} commandes")
        
        # Afficher les premières lignes
        print("\nAPERÇU DES DONNEES:")
        df.select("order_id", "date", "status", "total_amount").limit(5).show()
        
        # 3. Préparer les données
        df_features = prepare_data(df)
        if df_features is None:
            return
        
        # 4. Entraîner le modèle
        model, spark = train_model(df_features, spark)
        if model is None:
            return
        
        # 5. Faire des prédictions
        predictions = make_predictions(model, spark, days_ahead)
        if predictions is None:
            return
        
        # 6. Sauvegarder dans MongoDB
        if not save_to_mongodb(predictions):
            print("\nLes prédictions n'ont pas été sauvegardées dans MongoDB")
            print("Mais vous pouvez quand même les visualiser ci-dessus")
        
        print("\n" + "="*70)
        print("PREDICTION TERMINEE AVEC SUCCES!")
        print("="*70)
        print("Conseil: Exécutez 'diagrammes.py' pour voir les visualisations")
        
    except Exception as e:
        print(f"\nErreur inattendue: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        if spark:
            spark.stop()
            print("\nSession Spark fermée")
if __name__ == "__main__":
    main()