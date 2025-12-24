import os
import sys

# saretli barcha machakel 
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['PYSPARK_SUBMIT_ARGS'] = '--master local[*] pyspark-shell'
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

if 'JAVA_TOOL_OPTIONS' in os.environ:
    del os.environ['JAVA_TOOL_OPTIONS']
if '_JAVA_OPTIONS' in os.environ:
    del os.environ['_JAVA_OPTIONS']


# importer l biblios
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, when
from pyspark.sql.functions import min as spark_min, max as spark_max, sum as spark_sum, avg as spark_avg, count as spark_count
import logging
from datetime import datetime
import pandas as pd
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session(app_name="MongoDBConnection"):
    """Crée et configure une session Spark"""
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .master("local[*]") \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:10.4.0") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        spark.sparkContext.setLogLevel("WARN")
        logger.info(f"Spark session créée avec succès - Version: {pyspark.__version__}")
        return spark
        
    except Exception as e:
        logger.error(f"Erreur lors de la création de la session Spark: {str(e)}")
        raise

def load_mongo_data(spark, uri, database, collection):
    """Charger l données mn MongoDB"""
    try:
        logger.info(f"Chargement des données depuis {database}.{collection}")
        
        df = spark.read \
            .format("mongodb") \
            .option("uri", uri) \
            .option("database", database) \
            .option("collection", collection) \
            .load()
        
        count = df.count()
        logger.info(f"Données chargées avec succès: {count} enregistrements")
        return df
        
    except Exception as e:
        logger.error(f"Erreur lors du chargement des données: {str(e)}")
        return None

def display_dataframe_nicely(df, collection_name, limit=20):
    """Affiche un DataFrame de manière lisible"""
    print(f"\n{'='*100}")
    print(f"COLLECTION: {collection_name} - Aperçu des données")
    print(f"{'='*100}")
    
    if df is None or df.count() == 0:
        print("Aucune donnée disponible")
        return
    
    # Compter les lignes et colonnes
    row_count = df.count()
    col_count = len(df.columns)
    
    print(f"Dimensions: {row_count} lignes × {col_count} colonnes")
    print(f"Aperçu des {min(limit, row_count)} premières lignes:")
    print("-" * 100)
    
    # Afficher le schéma
    print("SCHEMA:")
    df.printSchema()
    
    print("\nDONNEES:")
    
    # Pour toutes les collections, utiliser toPandas pour un meilleur affichage
    try:
        # Convertir en Pandas
        pdf = df.limit(limit).toPandas()
        
        # Ajuster les options d'affichage Pandas
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        
        # Ajuster la largeur des colonnes selon la collection
        if collection_name.lower() == "orders":
            pd.set_option('display.max_colwidth', 40)
        else:
            pd.set_option('display.max_colwidth', 25)
        
        print(pdf.to_string(index=False))
        
        # Pour orders, afficher aussi un résumé des articles
        if collection_name.lower() == "orders":
            print("\nRESUME DES ARTICLES (premières commandes):")
            try:
                for i, row in pdf.head(3).iterrows():
                    items = row.get('items', [])
                    if items and len(items) > 0:
                        print(f"\nCommande {i+1}:")
                        for item in items[:2]:  # Max 2 articles
                            if isinstance(item, dict):
                                name = item.get('name', 'Inconnu')
                                qty = item.get('quantity', 0)
                                price = item.get('price', 0)
                                print(f"  - {name[:30]} (x{qty}) - {price} TND")
            except:
                pass
                
    except Exception as e:
        # Fallback à l'affichage Spark standard
        print(f"Note: Affichage simplifié (erreur Pandas: {str(e)})")
        df.limit(limit).show(truncate=30, vertical=False)
    
    print("-" * 100)

def analyze_customers(df):
    """Analyse spécifique pour la collection customers"""
    print("\nANALYSE DES CLIENTS:")
    
    # Distribution par ville
    print("DISTRIBUTION PAR VILLE:")
    try:
        if "location" in df.columns:
            city_dist = df.groupBy("location").agg(
                spark_count("*").alias("Nombre_Clients")
            ).orderBy(col("Nombre_Clients").desc())
            city_dist.show(truncate=False)
        else:
            print("  Colonne 'location' non trouvée")
    except Exception as e:
        print(f"  Erreur: {str(e)}")
    
    # Dates d'inscription
    print("\nPERIODE D'INSCRIPTION:")
    try:
        if "join_date" in df.columns:
            dates_df = df.agg(
                spark_min(col("join_date")).alias("Premier_inscrit"),
                spark_max(col("join_date")).alias("Dernier_inscrit")
            ).collect()[0]
            
            print(f"  Premier client inscrit: {dates_df['Premier_inscrit']}")
            print(f"  Dernier client inscrit: {dates_df['Dernier_inscrit']}")
        else:
            print("  Colonne 'join_date' non trouvée")
    except Exception as e:
        print(f"  Erreur: {str(e)}")
    
    # Statistiques générales
    print("\nSTATISTIQUES GENERALES:")
    try:
        total_clients = df.count()
        cities_count = df.select("location").distinct().count() if "location" in df.columns else 0
        
        print(f"  Nombre total de clients: {total_clients}")
        print(f"  Villes différentes: {cities_count}")
    except Exception as e:
        print(f"  Erreur: {str(e)}")

def analyze_orders(df):
    """Analyse spécifique pour la collection orders"""
    print("\nANALYSE DES COMMANDES:")
    
    # Statistiques générales
    try:
        stats_df = df.agg(
            spark_count("*").alias("Total_Commandes"),
            spark_sum(col("total_amount")).alias("Chiffre_Affaires_Total"),
            spark_avg(col("total_amount")).alias("Panier_Moyen")
        ).collect()[0]
        
        total_sales = stats_df["Chiffre_Affaires_Total"] or 0
        avg_order = stats_df["Panier_Moyen"] or 0
        
        print(f"Chiffre d'affaires total: {total_sales:,.2f} TND")
        print(f"Valeur moyenne par commande: {avg_order:,.2f} TND")
        print(f"Nombre total de commandes: {stats_df['Total_Commandes']}")
    except Exception as e:
        print(f"  Erreur statistiques: {str(e)}")
    
    # Distribution par statut
    print("\nDISTRIBUTION PAR STATUT:")
    try:
        if "status" in df.columns:
            status_dist = df.groupBy("status").agg(
                spark_count("*").alias("nombre"),
                spark_avg("total_amount").alias("moyenne_TND")
            ).orderBy(col("nombre").desc())
            status_dist.show(truncate=False)
        else:
            print("  Colonne 'status' non trouvée")
    except Exception as e:
        print(f"  Erreur: {str(e)}")
    
    # Distribution par méthode de paiement
    print("\nDISTRIBUTION PAR METHODE DE PAIEMENT:")
    try:
        if "payment_method" in df.columns:
            payment_dist = df.groupBy("payment_method").agg(
                spark_count("*").alias("nombre"),
                spark_sum("total_amount").alias("total_TND")
            ).orderBy(col("total_TND").desc())
            payment_dist.show(truncate=False)
        else:
            print("  Colonne 'payment_method' non trouvée")
    except Exception as e:
        print(f"  Erreur: {str(e)}")
    
    # Période couverte
    print("\nPERIODE DES COMMANDES:")
    try:
        if "date" in df.columns:
            period_df = df.agg(
                spark_min(col("date")).alias("Premiere_commande"),
                spark_max(col("date")).alias("Derniere_commande")
            ).collect()[0]
            
            print(f"  Première commande: {period_df['Premiere_commande']}")
            print(f"  Dernière commande: {period_df['Derniere_commande']}")
        else:
            print("  Colonne 'date' non trouvée")
    except Exception as e:
        print(f"  Erreur: {str(e)}")

def analyze_products(df):
    """Analyse spécifique pour la collection products"""
    print("\nANALYSE DES PRODUITS:")
    
    try:
        # Vérifier les colonnes existantes
        available_cols = df.columns
        print(f"  Colonnes disponibles: {', '.join(available_cols)}")
        
        # Statistiques de prix si la colonne existe
        if "price" in available_cols:
            price_stats = df.agg(
                spark_count("*").alias("Nombre_Produits"),
                spark_avg("price").alias("Prix_Moyen"),
                spark_min("price").alias("Prix_Min"),
                spark_max("price").alias("Prix_Max")
            ).collect()[0]
            
            print(f"Prix moyen: {price_stats['Prix_Moyen']:,.2f} TND")
            print(f"Prix minimum: {price_stats['Prix_Min']:,.2f} TND")
            print(f"Prix maximum: {price_stats['Prix_Max']:,.2f} TND")
            print(f"Nombre de produits: {price_stats['Nombre_Produits']}")
        
        # Distribution par catégorie si la colonne existe
        if "category" in available_cols:
            print("\nDISTRIBUTION PAR CATEGORIE:")
            cat_dist = df.groupBy("category").agg(
                spark_count("*").alias("count")
            ).orderBy(col("count").desc())
            cat_dist.show(truncate=False)
            
        # Distribution par disponibilité si la colonne existe
        if "available" in available_cols:
            print("\nDISPONIBILITE:")
            avail_dist = df.groupBy("available").agg(
                spark_count("*").alias("count")
            )
            avail_dist.show(truncate=False)
            
    except Exception as e:
        print(f"  Erreur: {str(e)}")

def analyze_data(df, collection_name):
    """Analyse basique des données"""
    if df is None:
        print("Aucune donnée à analyser")
        return
    
    print(f"\n{'='*60}")
    print(f"ANALYSE DE LA COLLECTION: {collection_name}")
    print(f"{'='*60}")
    
    # Informations de base
    row_count = df.count()
    col_count = len(df.columns)
    print(f"Dimensions: {row_count} lignes × {col_count} colonnes")
    
    # Afficher les données de manière formatée
    display_dataframe_nicely(df, collection_name)
    
    # Statistiques descriptives pour les collections spécifiques
    if collection_name.lower() == "customers":
        analyze_customers(df)
    elif collection_name.lower() == "orders":
        analyze_orders(df)
    elif collection_name.lower() == "products":
        analyze_products(df)
    
    # Valeurs manquantes
    print("\nVALEURS MANQUANTES:")
    try:
        for column in df.columns:
            missing_count = df.filter(col(column).isNull()).count()
            if missing_count > 0:
                percent = (missing_count / row_count * 100) if row_count > 0 else 0
                print(f"  {column}: {missing_count} ({percent:.1f}%)")
    except Exception as e:
        print(f"  Erreur dans l'analyse des valeurs manquantes: {str(e)}")
    
    return df

def export_as_json(df, collection_name, output_file=None):
    """Exporte les données au format JSON similaire à MongoDB"""
    if output_file is None:
        output_file = f"{collection_name}_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    print(f"\nExport des données au format JSON vers {output_file}...")
    
    try:
        # Collecter toutes les données
        rows = df.collect()
        
        # Convertir les lignes en dictionnaires
        data_list = []
        for row in rows:
            # Utiliser asDict() pour convertir la Row en dictionnaire
            if hasattr(row, 'asDict'):
                row_dict = row.asDict()
            else:
                # Fallback si asDict n'est pas disponible
                row_dict = {}
                for field in df.columns:
                    row_dict[field] = row[field]
            
            # Traiter les ObjectId MongoDB si présents
            if '_id' in row_dict and hasattr(row_dict['_id'], 'asDict'):
                # Convertir ObjectId en chaîne
                row_dict['_id'] = str(row_dict['_id'])
            
            data_list.append(row_dict)
        
        # Écrire dans le fichier JSON
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data_list, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"Données exportées dans {output_file}")
        print(f"Nombre d'enregistrements exportés: {len(data_list)}")
        
        # Afficher un aperçu du fichier JSON
        print(f"\nAperçu du fichier JSON:")
        print(f"    Taille du fichier: {os.path.getsize(output_file):,} octets")
        print(f"    Format: JSON avec {len(data_list)} objets")
        
        # Afficher le premier enregistrement comme exemple
        if data_list:
            print(f"\n    Exemple du premier enregistrement:")
            first_record = json.dumps(data_list[0], indent=2, ensure_ascii=False, default=str)
            lines = first_record.split('\n')
            for line in lines[:10]:  # Afficher seulement les 10 premières lignes
                print(f"      {line}")
            if len(lines) > 10:
                print(f"      ... (et {len(lines)-10} lignes supplémentaires)")
        
        return output_file
        
    except Exception as e:
        print(f"Erreur lors de l'export JSON: {str(e)}")
        return None

def interactive_menu(spark):
    """Menu interactif"""
    MONGO_URI = "mongodb://localhost:27017"
    DATABASE = "Boutique"
    
    while True:
        print("\n" + "-"*80)
        print("MENU INTERACTIF - ANALYSE DE BOUTIQUE")
        print("-"*80)
        print("1. Analyser les clients")
        print("2. Analyser les commandes")
        print("3. Analyser les produits")
        print("4. Statistiques complètes")
        print("5. Exporter les données")
        print("6. Rechercher")
        print("7. Quitter")
        
        choice = input("\nVotre choix (1-7): ").strip()
        
        try:
            if choice == "1":
                df = load_mongo_data(spark, MONGO_URI, DATABASE, "customers")
                if df:
                    analyze_data(df, "customers")
                    
            elif choice == "2":
                df = load_mongo_data(spark, MONGO_URI, DATABASE, "orders")
                if df:
                    analyze_data(df, "orders")
                    
            elif choice == "3":
                df = load_mongo_data(spark, MONGO_URI, DATABASE, "products")
                if df:
                    analyze_data(df, "products")
                    
            elif choice == "4":
                print("\nSTATISTIQUES COMPLETES")
                print("-" * 40)
                collections = ["customers", "orders", "products"]
                for collection in collections:
                    df = load_mongo_data(spark, MONGO_URI, DATABASE, collection)
                    if df:
                        print(f"\n{collection.upper()}:")
                        print(f"  Nombre d'enregistrements: {df.count():,}")
                        print(f"  Colonnes: {len(df.columns)}")
                        
            elif choice == "5":
                collection = input("Collection à exporter (customers/orders/products): ").strip().lower()
                if collection in ["customers", "orders", "products"]:
                    df = load_mongo_data(spark, MONGO_URI, DATABASE, collection)
                    if df:
                        filename = f"{collection}_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                        export_as_json(df, collection, filename)
                else:
                    print("Collection invalide !!!!")
                    
            elif choice == "6":
                collection = input("Collection à rechercher (customers/orders/products): ").strip().lower()
                if collection in ["customers", "orders", "products"]:
                    df = load_mongo_data(spark, MONGO_URI, DATABASE, collection)
                    if df:
                        search_field = input("Champ à rechercher: ").strip()
                        search_value = input("Valeur à rechercher: ").strip()
                        
                        if search_field in df.columns:
                            results = df.filter(col(search_field).contains(search_value))
                            result_count = results.count()
                            print(f"\n {result_count} résultat(s) trouvé(s):")
                            if result_count > 0:
                                results.show(truncate=True)
                        else:
                            print(f"Champ '{search_field}' non trouvé")
                            print(f"   Champs disponibles: {', '.join(df.columns)}")
                else:
                    print("Collection invalide")
                    
            elif choice == "7":
                print("\nFermeture de l'application...")
                break
                
            else:
                print("Choix invalide")
                
        except Exception as e:
            print(f"Erreur: {str(e)}")
            import traceback
            traceback.print_exc()

def main():
    """Fonction principale"""
    try:
        print("\n" + "-"*80)
        print("TAKE OFF DE L'APPLICATION SPARK-MONGODB")
        print("-"*80)
        print(f"Python version: {sys.version.split()[0]}")
        print(f"Python executable: {sys.executable}")
        print(f"PySpark version: {pyspark.__version__}")
        
        # Créer la session Spark
        spark = create_spark_session("BoutiqueAnalyzer")
        
        # Menu interactif
        interactive_menu(spark)
        
    except KeyboardInterrupt:
        print("\n\nInterruption par l'utilisateur")
    except Exception as e:
        print(f"\nErreur inattendue: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        if 'spark' in locals():
            spark.stop()
            print("\nSession Spark fermée")

if __name__ == "__main__":
    main()