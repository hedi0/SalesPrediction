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
from pyspark.sql.functions import (
    col, sum, count, date_format, explode, 
    when, avg, max, min, round, desc, 
    year, month, dayofmonth, dayofweek,
    countDistinct, current_date
)
from pyspark.sql.window import Window
from pyspark.sql.types import DateType, StringType
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BoutiqueAnalyzer:
    """Classe principale pour l'analyse de la boutique"""
    
    def __init__(self):
        # Définir les attributs d'abord
        self.mongo_uri = "mongodb://localhost:27017"
        self.database = "Boutique"
        self.spark = self._create_spark_session()
        
    def _create_spark_session(self):
        """Crée et configure la session Spark"""
        spark = SparkSession.builder \
            .appName("BoutiqueAdvancedAnalysis") \
            .master("local[*]") \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:10.4.0") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true") \
            .config("spark.local.dir", "C:\\Temp\\spark_temp") \
            .config("spark.mongodb.read.connection.uri", f"mongodb://localhost:27017/{self.database}") \
            .config("spark.mongodb.write.connection.uri", f"mongodb://localhost:27017/{self.database}") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("Session Spark créée avec succès")
        return spark
    
    def load_data(self):
        """Charge toutes les données nécessaires"""
        logger.info("Chargement des données depuis MongoDB...")
        
        try:
            # Méthode 1: Utiliser les options séparées
            logger.info("Tentative méthode 1...")
            self.orders_df = self.spark.read \
                .format("mongodb") \
                .option("uri", self.mongo_uri) \
                .option("database", self.database) \
                .option("collection", "orders") \
                .load()
            
            self.products_df = self.spark.read \
                .format("mongodb") \
                .option("uri", self.mongo_uri) \
                .option("database", self.database) \
                .option("collection", "products") \
                .load()
            
            self.customers_df = self.spark.read \
                .format("mongodb") \
                .option("uri", self.mongo_uri) \
                .option("database", self.database) \
                .option("collection", "customers") \
                .load()
                
            logger.info("Méthode 1 réussie")
                
        except Exception as e:
            logger.error(f"Erreur avec la méthode 1: {e}")
            logger.info("Tentative avec la méthode 2...")
            
            try:
                # Méthode 2: Utiliser l'URI complet dans une seule option
                logger.info("Tentative méthode 2...")
                self.orders_df = self.spark.read \
                    .format("mongodb") \
                    .option("uri", f"{self.mongo_uri}/{self.database}.orders") \
                    .load()
                
                self.products_df = self.spark.read \
                    .format("mongodb") \
                    .option("uri", f"{self.mongo_uri}/{self.database}.products") \
                    .load()
                
                self.customers_df = self.spark.read \
                    .format("mongodb") \
                    .option("uri", f"{self.mongo_uri}/{self.database}.customers") \
                    .load()
                    
                logger.info("Méthode 2 réussie")
                    
            except Exception as e2:
                logger.error(f"Erreur avec la méthode 2: {e2}")
                logger.info("Tentative avec la méthode 3...")
                
                try:
                    # Méthode 3: Utiliser la configuration avec préfixe
                    logger.info("Tentative méthode 3...")
                    self.orders_df = self.spark.read \
                        .format("mongodb") \
                        .option("spark.mongodb.input.uri", f"{self.mongo_uri}/{self.database}.orders") \
                        .load()
                    
                    self.products_df = self.spark.read \
                        .format("mongodb") \
                        .option("spark.mongodb.input.uri", f"{self.mongo_uri}/{self.database}.products") \
                        .load()
                    
                    self.customers_df = self.spark.read \
                        .format("mongodb") \
                        .option("spark.mongodb.input.uri", f"{self.mongo_uri}/{self.database}.customers") \
                        .load()
                        
                    logger.info("Méthode 3 réussie")
                        
                except Exception as e3:
                    logger.error(f"Erreur avec la méthode 3: {e3}")
                    logger.error("Toutes les méthodes de chargement ont échoué")
                    
                    # Créer des DataFrames vides pour éviter les erreurs
                    self.orders_df = self.spark.createDataFrame([], schema=None)
                    self.products_df = self.spark.createDataFrame([], schema=None)
                    self.customers_df = self.spark.createDataFrame([], schema=None)
                    return
        
        # Vérification et log des résultats
        if hasattr(self, 'orders_df') and self.orders_df is not None:
            orders_count = self.orders_df.count()
            logger.info(f"Commandes chargées: {orders_count:,} enregistrements")
            
            if orders_count > 0:
                logger.info("Schéma des commandes:")
                self.orders_df.printSchema()
                
                logger.info("Exemple de données de commandes (3 premières lignes):")
                self.orders_df.limit(3).show(truncate=False)
            else:
                logger.warning("Collection 'orders' chargée mais vide")
        else:
            logger.error("Impossible de charger les commandes")
            self.orders_df = self.spark.createDataFrame([], schema=None)
        
        if hasattr(self, 'products_df') and self.products_df is not None:
            products_count = self.products_df.count()
            logger.info(f"Produits chargés: {products_count:,} enregistrements")
            
            if products_count > 0:
                logger.info("Exemple de produits (3 premières lignes):")
                self.products_df.limit(3).show(truncate=False)
        else:
            logger.error("Impossible de charger les produits")
            self.products_df = self.spark.createDataFrame([], schema=None)
        
        if hasattr(self, 'customers_df') and self.customers_df is not None:
            customers_count = self.customers_df.count()
            logger.info(f"Clients chargés: {customers_count:,} enregistrements")
            
            if customers_count > 0:
                logger.info("Exemple de clients (3 premières lignes):")
                self.customers_df.limit(3).show(truncate=False)
        else:
            logger.error("Impossible de charger les clients")
            self.customers_df = self.spark.createDataFrame([], schema=None)
    
    def clean_and_prepare_data(self):
        """Nettoie et prépare les données pour l'analyse"""
        logger.info("Nettoyage et préparation des données...")
        
        # Utiliser toutes les commandes si la DataFrame existe
        if self.orders_df.count() > 0:
            self.orders_cleaned = self.orders_df.alias("orders_cleaned")
            
            # Vérifier et afficher les valeurs uniques dans 'status' pour le débogage
            if 'status' in self.orders_cleaned.columns:
                logger.info("Valeurs uniques dans 'status':")
                self.orders_cleaned.select("status").distinct().show()
                
                # Filtrer les commandes livrées
                self.orders_cleaned = self.orders_cleaned.filter(
                    (col("status").like("%livr%")) |
                    (col("status").like("%Livr%")) |
                    (col("status").like("%delivered%")) |
                    (col("status").like("%Delivered%"))
                )
            else:
                logger.warning("Colonne 'status' non trouvée, pas de filtrage")
            
            # Préparation des dates
            if 'date' in self.orders_cleaned.columns:
                try:
                    # Essayer de convertir la date
                    self.orders_cleaned = self.orders_cleaned \
                        .withColumn("date_formatted", col("date").cast(DateType())) \
                        .withColumn("year", year(col("date_formatted"))) \
                        .withColumn("month", month(col("date_formatted"))) \
                        .withColumn("day", dayofmonth(col("date_formatted"))) \
                        .withColumn("weekday", dayofweek(col("date_formatted"))) \
                        .withColumn("is_weekend", when(col("weekday").isin([1, 7]), 1).otherwise(0))
                    
                    self.orders_cleaned = self.orders_cleaned \
                        .withColumn("month_year", date_format(col("date_formatted"), "yyyy-MM"))
                        
                    logger.info("Dates formatées avec succès")
                        
                except Exception as e:
                    logger.error(f"Erreur lors du formatage des dates: {e}")
                    # Utiliser une date par défaut
                    self.orders_cleaned = self.orders_cleaned \
                        .withColumn("date_formatted", current_date()) \
                        .withColumn("year", year(current_date())) \
                        .withColumn("month", month(current_date())) \
                        .withColumn("day", dayofmonth(current_date())) \
                        .withColumn("weekday", dayofweek(current_date())) \
                        .withColumn("is_weekend", when(col("weekday").isin([1, 7]), 1).otherwise(0)) \
                        .withColumn("month_year", date_format(current_date(), "yyyy-MM"))
                        
                    logger.info("Dates par défaut utilisées")
            else:
                logger.warning("Colonne 'date' non trouvée")
                # Créer des colonnes de date par défaut
                self.orders_cleaned = self.orders_cleaned \
                    .withColumn("date_formatted", current_date()) \
                    .withColumn("year", year(current_date())) \
                    .withColumn("month", month(current_date())) \
                    .withColumn("day", dayofmonth(current_date())) \
                    .withColumn("weekday", dayofweek(current_date())) \
                    .withColumn("is_weekend", when(col("weekday").isin([1, 7]), 1).otherwise(0)) \
                    .withColumn("month_year", date_format(current_date(), "yyyy-MM"))
                    
                logger.info("Colonnes de date par défaut ajoutées")
        else:
            logger.warning("Aucune commande disponible")
            # Créer une DataFrame vide avec les bonnes colonnes
            schema = None
            self.orders_cleaned = self.spark.createDataFrame([], schema=schema)
        
        logger.info(f"Commandes préparées: {self.orders_cleaned.count():,}")
        
        # Afficher les colonnes disponibles
        logger.info(f"Colonnes disponibles dans orders_cleaned: {self.orders_cleaned.columns}")
    
    def calculate_basic_statistics(self):
        """Calcule les statistiques de base"""
        logger.info("\n" + "="*60)
        logger.info("STATISTIQUES GENERALES")
        logger.info("="*60)
        
        total_orders = self.orders_df.count()
        delivered_orders = self.orders_cleaned.count()
        total_customers = self.customers_df.count()
        total_products = self.products_df.count()
        
        logger.info(f"Commandes totales: {total_orders:,}")
        logger.info(f"Commandes préparées: {delivered_orders:,}")
        
        if total_orders > 0:
            logger.info(f"Pourcentage: {delivered_orders/total_orders*100:.1f}%")
        
        logger.info(f"Clients uniques: {total_customers:,}")
        logger.info(f"Produits disponibles: {total_products:,}")
        
        # Calculer le chiffre d'affaires si possible
        if 'total_amount' in self.orders_cleaned.columns:
            try:
                total_revenue_result = self.orders_cleaned.agg(sum("total_amount")).collect()[0][0]
                avg_order_result = self.orders_cleaned.agg(avg("total_amount")).collect()[0][0]
                
                total_revenue = total_revenue_result if total_revenue_result else 0
                avg_order_value = avg_order_result if avg_order_result else 0
                
                logger.info(f"Chiffre d'affaires total: {total_revenue:,.2f} TND")
                logger.info(f"Valeur moyenne par commande: {avg_order_value:,.2f} TND")
            except Exception as e:
                logger.error(f"Erreur lors du calcul du CA: {e}")
        else:
            logger.info("Colonne 'total_amount' non trouvée - calcul du CA impossible")
        
        # Répartition par statut si disponible
        if 'status' in self.orders_df.columns and total_orders > 0:
            logger.info("\nREPARTITION PAR STATUT:")
            status_dist = self.orders_df.groupBy("status") \
                .agg(
                    count("*").alias("nombre_commandes"),
                    round((count("*") / total_orders * 100), 2).alias("pourcentage")
                ) \
                .orderBy(desc("nombre_commandes"))
            
            status_dist.show(truncate=False, n=10)
        else:
            logger.info("Colonne 'status' non trouvée ou aucune commande")
    
    def analyze_daily_sales(self):
        """Analyse des ventes quotidiennes"""
        logger.info("\n" + "="*60)
        logger.info("ANALYSE DES VENTES QUOTIDIENNES")
        logger.info("="*60)
        
        if self.orders_cleaned.count() == 0:
            logger.warning("Aucune donnée à analyser")
            return None
        
        try:
            # Grouper par date
            sales_by_day = self.orders_cleaned.groupBy("date_formatted") \
                .agg(
                    count("*").alias("nb_commandes"),
                    countDistinct("customer_id").alias("clients_uniques")
                ) \
                .withColumnRenamed("date_formatted", "date_jour") \
                .orderBy("date_jour")
            
            # Ajouter les montants si disponible
            if 'total_amount' in self.orders_cleaned.columns:
                # Utiliser une fenêtre pour les calculs agrégés
                window_spec = Window.partitionBy("date_formatted")
                
                temp_df = self.orders_cleaned \
                    .withColumn("total_ventes_day", sum("total_amount").over(window_spec)) \
                    .withColumn("panier_moyen_day", avg("total_amount").over(window_spec))
                
                # Prendre les valeurs uniques par jour
                sales_by_day_with_amounts = temp_df.select(
                    col("date_formatted").alias("date_jour_amount"),
                    col("total_ventes_day"),
                    col("panier_moyen_day")
                ).distinct()
                
                # Joindre avec les comptes
                sales_by_day = sales_by_day.join(
                    sales_by_day_with_amounts,
                    col("date_jour") == col("date_jour_amount"),
                    "left"
                ).drop("date_jour_amount")
            
            # Calculer les statistiques globales
            if sales_by_day.count() > 0:
                daily_stats = sales_by_day.agg(
                    avg("nb_commandes").alias("commandes_moyennes_par_jour"),
                    max("nb_commandes").alias("max_commandes_jour"),
                    min("nb_commandes").alias("min_commandes_jour"),
                    count("date_jour").alias("nombre_jours_analyses")
                ).collect()[0]
                
                logger.info(f"Nombre de jours analysés: {daily_stats['nombre_jours_analyses']}")
                logger.info(f"Commandes moyennes par jour: {daily_stats['commandes_moyennes_par_jour']:.1f}")
                logger.info(f"Jour record: {daily_stats['max_commandes_jour']} commandes")
                logger.info(f"Jour le plus calme: {daily_stats['min_commandes_jour']} commandes")
            
            logger.info("\nPREMIERS ET DERNIERS JOURS:")
            sales_by_day.orderBy("date_jour").show(5, truncate=False)
            
            logger.info("\nTOP 5 MEILLEURS JOURS:")
            if 'total_ventes_day' in sales_by_day.columns:
                sales_by_day.orderBy(desc("total_ventes_day")).select(
                    "date_jour", "nb_commandes", "total_ventes_day", "panier_moyen_day"
                ).show(5, truncate=False)
            else:
                sales_by_day.orderBy(desc("nb_commandes")).select(
                    "date_jour", "nb_commandes", "clients_uniques"
                ).show(5, truncate=False)
            
            return sales_by_day
            
        except Exception as e:
            logger.error(f"Erreur lors de l'analyse quotidienne: {e}")
            return None
    
    def analyze_product_sales(self):
        """Analyse des ventes par produit"""
        logger.info("\n" + "="*60)
        logger.info("ANALYSE DES PRODUITS")
        logger.info("="*60)
        
        if self.orders_cleaned.count() == 0:
            logger.warning("Aucune donnée à analyser")
            return None
        
        try:
            # Vérifier si nous avons des items
            if 'items' not in self.orders_cleaned.columns:
                logger.info("Colonne 'items' non trouvée - tentative alternative")
                
                # Essayer avec products_df
                if self.products_df.count() > 0:
                    logger.info("STATISTIQUES DES PRODUITS:")
                    self.products_df.select("product_id", "name", "price", "category").limit(10).show(truncate=False)
                    return self.products_df.select("product_id", "name", "price", "category").limit(100)
                return None
            
            # Exploser les items
            exploded_items = self.orders_cleaned.select(
                explode(col("items")).alias("item")
            )
            
            # Vérifier la structure des items
            logger.info("Schéma des items explosés:")
            exploded_items.printSchema()
            
            # Grouper par produit
            product_sales = exploded_items.groupBy(
                col("item.product_id").alias("product_id"),
                col("item.name").alias("produit")
            ) \
            .agg(
                count("*").alias("nb_ventes"),
                sum(col("item.quantity")).alias("quantite_totale")
            ) \
            .orderBy(desc("nb_ventes"))
            
            logger.info(f"Total produits vendus: {product_sales.count()}")
            
            logger.info("\nTOP 10 PRODUITS LES PLUS VENDUS:")
            product_sales.show(10, truncate=False)
            
            return product_sales.limit(100)
            
        except Exception as e:
            logger.error(f"Erreur lors de l'analyse des produits: {e}")
            return None
    
    def analyze_customer_behavior(self):
        """Analyse du comportement des clients"""
        logger.info("\n" + "="*60)
        logger.info("ANALYSE DES CLIENTS")
        logger.info("="*60)
        
        if self.orders_cleaned.count() == 0:
            logger.warning("Aucune donnée à analyser")
            return None
        
        try:
            # Vérifier si nous avons customer_id
            if 'customer_id' not in self.orders_cleaned.columns:
                logger.warning("Colonne 'customer_id' non trouvée")
                return None
            
            # Analyser les clients
            customer_analysis = self.orders_cleaned.groupBy("customer_id") \
                .agg(
                    count("*").alias("nombre_commandes"),
                    countDistinct("date_formatted").alias("jours_achat")
                ) \
                .orderBy(desc("nombre_commandes"))
            
            logger.info(f"Total clients actifs: {customer_analysis.count()}")
            
            logger.info("\nTOP 10 CLIENTS PAR NOMBRE DE COMMANDES:")
            customer_analysis.limit(10).show(truncate=False)
            
            # Segmenter les clients
            if 'total_amount' in self.orders_cleaned.columns:
                customer_spending = self.orders_cleaned.groupBy("customer_id") \
                    .agg(
                        sum("total_amount").alias("total_depense"),
                        avg("total_amount").alias("panier_moyen")
                    )
                
                customer_analysis = customer_analysis.join(
                    customer_spending, "customer_id", "left"
                )
                
                # Segmentation
                customer_analysis = customer_analysis.withColumn("segment",
                    when(col("total_depense") >= 1000, "VIP")
                    .when(col("total_depense") >= 500, "Fidèle")
                    .when(col("total_depense") >= 100, "Actif")
                    .otherwise("Occasionnel")
                )
                
                logger.info("\nSEGMENTATION DES CLIENTS:")
                segmentation = customer_analysis.groupBy("segment") \
                    .agg(
                        count("*").alias("nb_clients"),
                        avg("total_depense").alias("depense_moyenne"),
                        avg("nombre_commandes").alias("commandes_moyennes")
                    ) \
                    .orderBy(desc("depense_moyenne"))
                
                segmentation.show(truncate=False)
            
            return customer_analysis.limit(100)
            
        except Exception as e:
            logger.error(f"Erreur lors de l'analyse des clients: {e}")
            return None
    
    def analyze_payment_methods(self):
        """Analyse des méthodes de paiement"""
        logger.info("\n" + "="*60)
        logger.info("ANALYSE DES METHODES DE PAIEMENT")
        logger.info("="*60)
        
        if self.orders_df.count() == 0:
            logger.warning("Aucune donnée à analyser")
            return None
        
        try:
            if 'payment_method' not in self.orders_df.columns:
                logger.info("Colonne 'payment_method' non trouvée")
                return None
            
            total_orders = self.orders_df.count()
            
            payment_analysis = self.orders_df.groupBy("payment_method") \
                .agg(
                    count("*").alias("nombre_commandes"),
                    round((count("*") / total_orders * 100), 2).alias("pourcentage")
                ) \
                .orderBy(desc("nombre_commandes"))
            
            logger.info("REPARTITION DES METHODES DE PAIEMENT:")
            payment_analysis.show(truncate=False)
            
            # Ajouter les montants si disponible
            if 'total_amount' in self.orders_df.columns:
                payment_amounts = self.orders_df.groupBy("payment_method") \
                    .agg(
                        sum("total_amount").alias("montant_total"),
                        avg("total_amount").alias("panier_moyen")
                    )
                
                payment_analysis = payment_analysis.join(
                    payment_amounts, "payment_method", "left"
                )
                
                logger.info("\nMONTANTS PAR METHODE DE PAIEMENT:")
                payment_analysis.select(
                    "payment_method", "nombre_commandes", "pourcentage",
                    "montant_total", "panier_moyen"
                ).show(truncate=False)
            
            return payment_analysis
            
        except Exception as e:
            logger.error(f"Erreur lors de l'analyse des méthodes de paiement: {e}")
            return None
    
    def save_results_to_mongodb(self, results_dict):
        """Sauvegarde les résultats dans MongoDB"""
        logger.info("\n" + "="*60)
        logger.info("SAUVEGARDE DES RESULTATS DANS MONGODB")
        logger.info("="*60)
        
        for result_name, df in results_dict.items():
            if df is not None and df.count() > 0:
                try:
                    collection_name = f"analysis_{result_name}"
                    
                    logger.info(f"Sauvegarde de {result_name} dans la collection {collection_name}...")
                    
                    df.write \
                        .format("mongodb") \
                        .mode("overwrite") \
                        .option("uri", self.mongo_uri) \
                        .option("database", self.database) \
                        .option("collection", collection_name) \
                        .save()
                    
                    logger.info(f"{result_name} sauvegardé avec succès ({df.count()} enregistrements)")
                    
                except Exception as e:
                    logger.error(f"Erreur lors de la sauvegarde de {result_name}: {str(e)}")
                    
                    # Essayer une méthode alternative
                    try:
                        logger.info(f"Tentative de sauvegarde alternative pour {result_name}...")
                        df.write \
                            .format("mongodb") \
                            .mode("overwrite") \
                            .option("spark.mongodb.output.uri", f"{self.mongo_uri}/{self.database}.{collection_name}") \
                            .save()
                        logger.info(f"{result_name} sauvegardé avec méthode alternative")
                    except Exception as e2:
                        logger.error(f"Méthode alternative échouée pour {result_name}: {str(e2)}")
            else:
                logger.info(f"{result_name}: aucune donnée à sauvegarder")
    
    def generate_summary_report(self):
        """Génère un rapport récapitulatif"""
        logger.info("\n" + "="*60)
        logger.info("RAPPORT DE SYNTHESE")
        logger.info("="*60)
        
        summary_data = []
        
        # Statistiques de base
        total_orders = self.orders_df.count()
        cleaned_orders = self.orders_cleaned.count()
        total_customers = self.customers_df.count()
        total_products = self.products_df.count()
        
        summary_data.append(f"COMMANDES TOTALES: {total_orders:,}")
        summary_data.append(f"COMMANDES NETTOYEES: {cleaned_orders:,}")
        
        if total_orders > 0:
            summary_data.append(f"TAUX D'UTILISATION: {cleaned_orders/total_orders*100:.1f}%")
        
        summary_data.append(f"CLIENTS INSCRITS: {total_customers:,}")
        summary_data.append(f"PRODUITS DISPONIBLES: {total_products:,}")
        
        # Clients actifs
        if 'customer_id' in self.orders_cleaned.columns and cleaned_orders > 0:
            active_customers = self.orders_cleaned.select(countDistinct("customer_id")).collect()[0][0]
            summary_data.append(f"CLIENTS ACTIFS: {active_customers:,}")
            
            if total_customers > 0:
                summary_data.append(f"TAUX D'ACTIVATION: {active_customers/total_customers*100:.1f}%")
        
        # Chiffre d'affaires
        if 'total_amount' in self.orders_cleaned.columns and cleaned_orders > 0:
            try:
                total_revenue = self.orders_cleaned.agg(sum("total_amount")).collect()[0][0] or 0
                avg_order_value = self.orders_cleaned.agg(avg("total_amount")).collect()[0][0] or 0
                
                summary_data.append(f"CHIFFRE D'AFFAIRES TOTAL: {total_revenue:,.2f} TND")
                summary_data.append(f"PANIER MOYEN: {avg_order_value:,.2f} TND")
            except:
                pass
        
        # Afficher le rapport
        for line in summary_data:
            logger.info(line)
    
    def run_full_analysis(self):
        """Exécute l'analyse complète"""
        try:
            logger.info("DEMARRAGE DE L'ANALYSE COMPLETE")
            logger.info("="*60)
            
            # Étape 1: Charger les données
            self.load_data()
            
            # Vérifier si nous avons des données
            if self.orders_df.count() == 0:
                logger.error("AUCUNE DONNEE TROUVEE")
                logger.info("VERIFICATIONS:")
                logger.info("  1. MongoDB est-il en cours d'exécution?")
                logger.info("  2. La base 'Boutique' existe-t-elle?")
                logger.info("  3. Les collections 'orders', 'products', 'customers' existent-elles?")
                logger.info("  4. Les collections contiennent-elles des données?")
                logger.info("Astuce: Exécutez 'mongo' puis 'use Boutique' et 'show collections' pour vérifier")
                return
            
            # Étape 2: Nettoyer et préparer
            self.clean_and_prepare_data()
            
            # Étape 3: Analyses
            self.calculate_basic_statistics()
            
            # Exécuter les analyses principales
            daily_sales = self.analyze_daily_sales()
            product_sales = self.analyze_product_sales()
            customer_analysis = self.analyze_customer_behavior()
            payment_analysis = self.analyze_payment_methods()
            
            # Étape 4: Sauvegarde des résultats
            results_dict = {}
            
            if daily_sales is not None:
                results_dict["daily_sales"] = daily_sales
            
            if product_sales is not None:
                results_dict["product_sales"] = product_sales
            
            if customer_analysis is not None:
                results_dict["customer_behavior"] = customer_analysis
            
            if payment_analysis is not None:
                results_dict["payment_methods"] = payment_analysis
            
            if results_dict:
                self.save_results_to_mongodb(results_dict)
            else:
                logger.warning("Aucun résultat à sauvegarder")
            
            # Étape 5: Rapport final
            self.generate_summary_report()
            
            logger.info("\n" + "="*60)
            logger.info("ANALYSE TERMINEE AVEC SUCCES")
            logger.info("="*60)
            
        except Exception as e:
            logger.error(f"Erreur lors de l'analyse: {str(e)}", exc_info=True)
            
        finally:
            if hasattr(self, 'spark'):
                try:
                    self.spark.stop()
                    logger.info("Session Spark fermée")
                except:
                    pass

def main():
    """Fonction principale"""
    logger.info(f"\nPython version: {sys.version.split()[0]}")
    logger.info(f"Python executable: {sys.executable}")
    
    try:
        analyzer = BoutiqueAnalyzer()
        analyzer.run_full_analysis()
    except Exception as e:
        logger.error(f"Erreur fatale: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()