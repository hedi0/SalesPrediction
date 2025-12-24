from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import numpy as np
import warnings
warnings.filterwarnings('ignore')

# Configuration du style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

def create_dashboard():
    print("\n" + "="*70)
    print("DASHBOARD D'ANALYSE - BOUTIQUE")
    print("="*70)
    
    try:
        client = MongoClient('localhost', 27017, serverSelectionTimeoutMS=5000)
        db = client['Boutique']

        client.server_info()
        print("Connecté à MongoDB")
        
    except Exception as e:
        print(f"Erreur de connexion MongoDB: {e}")
        print("Vérifiez que MongoDB est en cours d'exécution")
        return
    
    collections = db.list_collection_names()
    print(f"Collections disponibles: {collections}")
    
    # 1. STATISTIQUES GÉNÉRALES
    print("\n" + "="*70)
    print("1. STATISTIQUES GÉNÉRALES")
    print("="*70)
    
    try:
        # Commandes
        orders_count = db.orders.count_documents({})
        print(f"COMMANDES TOTALES: {orders_count:,}")
        
        # Commandes livrées
        delivered_count = db.orders.count_documents({"status": {"$regex": "livré", "$options": "i"}})
        print(f"COMMANDES LIVREES: {delivered_count:,} ({delivered_count/orders_count*100:.1f}%)")
        
        # Clients
        customers_count = db.customers.count_documents({})
        print(f"CLIENTS: {customers_count:,}")
        
        # Produits
        products_count = db.products.count_documents({})
        print(f"PRODUITS: {products_count:,}")
        
    except Exception as e:
        print(f"Erreur statistiques: {e}")
    
    # 2. VENTES ET REVENUS
    print("\n" + "="*70)
    print("2. VENTES ET REVENUS")
    print("="*70)
    
    try:
        pipeline = [
            {"$match": {"status": {"$regex": "livré", "$options": "i"}}},
            {"$group": {
                "_id": None,
                "total_sales": {"$sum": "$total_amount"},
                "avg_order": {"$avg": "$total_amount"},
                "max_order": {"$max": "$total_amount"},
                "min_order": {"$min": "$total_amount"}
            }}
        ]
        
        sales_stats = list(db.orders.aggregate(pipeline))
        
        if sales_stats:
            stats = sales_stats[0]
            print(f"CHIFFRE D'AFFAIRES: {stats['total_sales']:,.2f} TND")
            print(f"PANIER MOYEN: {stats['avg_order']:,.2f} TND")
            print(f"COMMANDE MAX: {stats['max_order']:,.2f} TND")
            print(f"COMMANDE MIN: {stats['min_order']:,.2f} TND")
        else:
            print("Aucune donnée de vente disponible")
            
    except Exception as e:
        print(f"Erreur ventes: {e}")
    
    # 3. ANALYSE DES PRODUITS
    print("\n" + "="*70)
    print("3. ANALYSE DES PRODUITS")
    print("="*70)
    
    try:
        # Top produits vendus
        pipeline = [
            {"$unwind": "$items"},
            {"$match": {"status": {"$regex": "livré", "$options": "i"}}},
            {"$group": {
                "_id": {"name": "$items.name", "id": "$items.product_id"},
                "total_sold": {"$sum": "$items.quantity"},
                "total_revenue": {"$sum": {"$multiply": ["$items.price", "$items.quantity"]}},
                "avg_price": {"$avg": "$items.price"}
            }},
            {"$sort": {"total_revenue": -1}},
            {"$limit": 5}
        ]
        
        top_products = list(db.orders.aggregate(pipeline))
        
        if top_products:
            print("TOP 5 PRODUITS PAR REVENU:")
            for i, product in enumerate(top_products, 1):
                name = product['_id']['name'] if 'name' in product['_id'] else "Inconnu"
                print(f"  {i}. {name[:25]:25} | {product['total_sold']:3} unités | {product['total_revenue']:8.2f} TND")
        else:
            print("Aucune donnée de produit disponible")
            
    except Exception as e:
        print(f"Erreur produits: {e}")
    
    # 4. ANALYSE DES CLIENTS
    print("\n" + "="*70)
    print("4. ANALYSE DES CLIENTS")
    print("="*70)
    
    try:
        # Top clients
        pipeline = [
            {"$match": {"status": {"$regex": "livré", "$options": "i"}}},
            {"$group": {
                "_id": "$customer_id",
                "total_spent": {"$sum": "$total_amount"},
                "order_count": {"$sum": 1},
                "avg_order": {"$avg": "$total_amount"}
            }},
            {"$sort": {"total_spent": -1}},
            {"$limit": 5}
        ]
        
        top_customers = list(db.orders.aggregate(pipeline))
        
        if top_customers:
            print("TOP 5 CLIENTS:")
            for i, customer in enumerate(top_customers, 1):
                # Chercher le nom du client
                client_info = db.customers.find_one({"customer_id": customer['_id']})
                if client_info and 'name' in client_info:
                    client_name = client_info['name']
                else:
                    client_name = f"Client {customer['_id'][-4:]}"
                
                print(f"  {i}. {client_name[:20]:20} | {customer['order_count']:2} commandes | {customer['total_spent']:8.2f} TND")
        else:
            print("Aucune donnée client disponible")
            
    except Exception as e:
        print(f"Erreur clients: {e}")
    
    # 5. MÉTHODES DE PAIEMENT
    print("\n" + "="*70)
    print("5. METHODES DE PAIEMENT")
    print("="*70)
    
    try:
        pipeline = [
            {"$group": {
                "_id": "$payment_method",
                "count": {"$sum": 1},
                "total_amount": {"$sum": "$total_amount"},
                "avg_amount": {"$avg": "$total_amount"}
            }},
            {"$sort": {"count": -1}}
        ]
        
        payment_methods = list(db.orders.aggregate(pipeline))
        
        if payment_methods:
            total_orders = sum([p['count'] for p in payment_methods])
            print("REPARTITION:")
            for payment in payment_methods:
                method = payment['_id'] if payment['_id'] else "Non spécifié"
                percentage = (payment['count'] / total_orders * 100) if total_orders > 0 else 0
                print(f"  {method:15} | {payment['count']:4} ({percentage:5.1f}%) | {payment['total_amount']:8.2f} TND")
        else:
            print("Aucune donnée de paiement disponible")
            
    except Exception as e:
        print(f"Erreur paiements: {e}")
    
    # 6. ÉVOLUTION TEMPORELLE
    print("\n" + "="*70)
    print("6. EVOLUTION TEMPORELLE")
    print("="*70)
    
    try:
        pipeline = [
            {"$match": {"status": {"$regex": "livré", "$options": "i"}}},
            {"$project": {
                "year_month": {"$dateToString": {"format": "%Y-%m", "date": "$date"}},
                "total_amount": 1,
                "date": 1
            }},
            {"$group": {
                "_id": "$year_month",
                "total_sales": {"$sum": "$total_amount"},
                "order_count": {"$sum": 1},
                "avg_sale": {"$avg": "$total_amount"}
            }},
            {"$sort": {"_id": 1}},
            {"$limit": 12}
        ]
        
        monthly_sales = list(db.orders.aggregate(pipeline))
        
        if monthly_sales:
            print("VENTES MENSUELLES:")
            for month_data in monthly_sales[-6:]:  # 6 derniers mois
                print(f"  {month_data['_id']}: {month_data['total_sales']:,.2f} TND ({month_data['order_count']} commandes)")
        else:
            print("Aucune donnée temporelle disponible")
            
    except Exception as e:
        print(f"Erreur temporelle: {e}")
    
    # 7. PRÉDICTIONS
    print("\n" + "="*70)
    print("7. PREDICTIONS MACHINE LEARNING")
    print("="*70)
    
    try:
        if "predictions" in collections:
            predictions = list(db.predictions.find().sort("predicted_date", 1))
            
            if predictions:
                print("PREDICTIONS FUTURES:")
                
                # Grouper par modèle
                predictions_df = pd.DataFrame(predictions)
                models = predictions_df['model'].unique() if 'model' in predictions_df.columns else ['LinearRegression']
                
                for model in models:
                    model_predictions = predictions_df[predictions_df['model'] == model] if 'model' in predictions_df.columns else predictions_df
                    print(f"\nModele: {model}")
                    
                    total_predicted = 0
                    for _, pred in model_predictions.iterrows():
                        date = pred.get('predicted_date', 'N/A')
                        amount = pred.get('predicted_sales', 0)
                        total_predicted += amount
                        
                        print(f"  {date}: {amount:,.2f} TND")
                    
                    if len(model_predictions) > 0:
                        avg = total_predicted / len(model_predictions)
                        print(f"  MOYENNE: {avg:,.2f} TND")
                        
                        # Comparaison avec historique
                        try:
                            # Ventes moyennes historiques
                            historical_avg = sales_stats[0]['avg_order'] if 'sales_stats' in locals() and sales_stats else 0
                            if historical_avg > 0:
                                diff_percent = ((avg - historical_avg) / historical_avg) * 100
                                trend = "HAUSSE" if diff_percent > 0 else "BAISSE"
                                print(f"  TREND: {trend} ({abs(diff_percent):.1f}% vs historique)")
                        except:
                            pass
            else:
                print("Aucune prédiction disponible")
                print("Exécutez 'prediction.py' pour générer des prédictions")
        else:
            print("Collection 'predictions' non trouvée")
            print("Exécutez 'prediction.py' pour générer des prédictions")
            
    except Exception as e:
        print(f"Erreur prédictions: {e}")
    
    # 8. CRÉATION DES VISUALISATIONS
    print("\n" + "="*70)
    print("CREATION DES GRAPHIQUES...")
    print("="*70)
    
    try:
        fig = plt.figure(figsize=(18, 12))
        fig.suptitle('DASHBOARD COMPLET - ANALYSE DE LA BOUTIQUE', fontsize=16, fontweight='bold')
        
        # Graphique 1: Top produits
        if 'top_products' in locals() and top_products:
            ax1 = plt.subplot(3, 3, 1)
            names = [p['_id']['name'][:15] if 'name' in p['_id'] else "Inconnu" for p in top_products]
            revenues = [p['total_revenue'] for p in top_products]
            
            bars = ax1.barh(names, revenues, color=plt.cm.Set3(range(len(names))))
            ax1.set_xlabel('Revenu (TND)')
            ax1.set_title('Top 5 Produits par CA')
            ax1.invert_yaxis()
            
            # Valeurs sur les barres
            for i, (bar, value) in enumerate(zip(bars, revenues)):
                ax1.text(value, bar.get_y() + bar.get_height()/2, 
                        f'{value:,.0f}', 
                        ha='left', va='center', fontweight='bold')
        
        # Graphique 2: Méthodes de paiement
        if 'payment_methods' in locals() and payment_methods:
            ax2 = plt.subplot(3, 3, 2)
            methods = [p['_id'] if p['_id'] else "Autre" for p in payment_methods]
            counts = [p['count'] for p in payment_methods]
            
            colors = plt.cm.Pastel1(range(len(methods)))
            wedges, texts, autotexts = ax2.pie(counts, labels=methods, autopct='%1.1f%%',
                                              colors=colors, startangle=90)
            ax2.set_title('Répartition des Paiements')
            
            # Style
            for autotext in autotexts:
                autotext.set_color('black')
                autotext.set_fontweight('bold')
        
        # Graphique 3: Évolution mensuelle
        if 'monthly_sales' in locals() and monthly_sales:
            ax3 = plt.subplot(3, 3, 3)
            months = [s['_id'][5:] for s in monthly_sales]  # Juste MM
            sales = [s['total_sales'] for s in monthly_sales]
            
            ax3.plot(months, sales, 'o-', linewidth=2, markersize=8)
            ax3.fill_between(months, sales, alpha=0.3)
            ax3.set_xlabel('Mois')
            ax3.set_ylabel('Ventes (TND)')
            ax3.set_title('Évolution des Ventes')
            ax3.tick_params(axis='x', rotation=45)
            ax3.grid(True, alpha=0.3)
            
            # Dernier point en évidence
            ax3.scatter(months[-1], sales[-1], color='red', s=100, zorder=5)
            ax3.annotate(f'{sales[-1]:,.0f}', 
                        xy=(months[-1], sales[-1]),
                        xytext=(months[-1], sales[-1] + max(sales)*0.1),
                        ha='center', fontweight='bold')
        
        # Graphique 4: Top clients
        if 'top_customers' in locals() and top_customers:
            ax4 = plt.subplot(3, 3, 4)
            
            # Préparer les noms
            client_names = []
            for customer in top_customers:
                client_info = db.customers.find_one({"customer_id": customer['_id']})
                if client_info and 'name' in client_info:
                    name = client_info['name']
                    if 'location' in client_info:
                        name = f"{name[:10]} ({client_info['location'][:3]})"
                    client_names.append(name[:12])
                else:
                    client_names.append(f"Client {customer['_id'][-4:]}")
            
            spending = [c['total_spent'] for c in top_customers]
            
            bars = ax4.bar(client_names, spending, color=plt.cm.Paired(range(len(client_names))))
            ax4.set_xlabel('Clients')
            ax4.set_ylabel('Dépenses (TND)')
            ax4.set_title('Top 5 Clients')
            ax4.tick_params(axis='x', rotation=45)
            
            # Valeurs
            for bar, value in zip(bars, spending):
                height = bar.get_height()
                ax4.text(bar.get_x() + bar.get_width()/2., height,
                        f'{value:,.0f}', ha='center', va='bottom', fontweight='bold')
        
        # Graphique 5: Prédictions
        if "predictions" in collections:
            ax5 = plt.subplot(3, 3, 5)
            
            predictions_data = list(db.predictions.find().sort("predicted_date", 1))
            if predictions_data:
                pred_dates = [p.get('predicted_date', '') for p in predictions_data]
                pred_sales = [p.get('predicted_sales', 0) for p in predictions_data]
                
                # Convertir en dates pour le plot
                dates = [datetime.strptime(d, "%Y-%m-%d") for d in pred_dates if d]
                
                if dates:
                    ax5.plot(dates, pred_sales, 's-', linewidth=2, markersize=8)
                    ax5.fill_between(dates, pred_sales, alpha=0.3)
                    ax5.set_xlabel('Date')
                    ax5.set_ylabel('Ventes prédites (TND)')
                    ax5.set_title('Prédictions Futures')
                    ax5.tick_params(axis='x', rotation=45)
                    ax5.grid(True, alpha=0.3)
        
        # Graphique 6: Distribution des commandes
        try:
            ax6 = plt.subplot(3, 3, 6)
            
            pipeline = [
                {"$match": {"status": {"$regex": "livré", "$options": "i"}}},
                {"$project": {"total_amount": 1}}
            ]
            
            orders_amounts = list(db.orders.aggregate(pipeline))
            amounts = [o['total_amount'] for o in orders_amounts if 'total_amount' in o]
            
            if amounts:
                ax6.hist(amounts, bins=15, edgecolor='black', alpha=0.7)
                ax6.set_xlabel('Montant (TND)')
                ax6.set_ylabel('Fréquence')
                ax6.set_title('Distribution des Montants')
                ax6.grid(True, alpha=0.3)
                
                # Ajouter lignes de moyenne
                mean_val = np.mean(amounts)
                median_val = np.median(amounts)
                ax6.axvline(mean_val, color='red', linestyle='--', label=f'Moyenne: {mean_val:,.0f}')
                ax6.axvline(median_val, color='green', linestyle='--', label=f'Mediane: {median_val:,.0f}')
                ax6.legend()
        except:
            pass
        
        # Graphique 7: Statut des commandes
        try:
            ax7 = plt.subplot(3, 3, 7)
            
            pipeline = [
                {"$group": {
                    "_id": "$status",
                    "count": {"$sum": 1}
                }}
            ]
            
            status_data = list(db.orders.aggregate(pipeline))
            if status_data:
                statuses = [s['_id'] if s['_id'] else "Inconnu" for s in status_data]
                counts = [s['count'] for s in status_data]
                
                colors = plt.cm.Set2(range(len(statuses)))
                ax7.pie(counts, labels=statuses, autopct='%1.1f%%', colors=colors)
                ax7.set_title('Statut des Commandes')
        except:
            pass
        
        # Graphique 8: Résumé statistique
        ax8 = plt.subplot(3, 3, 8)
        ax8.axis('off')
        
        # Collecter les statistiques pour le résumé
        summary_text = "RESUME STATISTIQUE\n\n"
        
        try:
            # Commandes
            summary_text += f"Commandes: {orders_count:,}\n"
            summary_text += f"Livrees: {delivered_count:,}\n"
            
            # CA
            if 'sales_stats' in locals() and sales_stats:
                summary_text += f"CA Total: {sales_stats[0]['total_sales']:,.0f} TND\n"
                summary_text += f"Panier Moyen: {sales_stats[0]['avg_order']:,.0f} TND\n"
            
            # Clients
            summary_text += f"Clients: {customers_count:,}\n"
            summary_text += f"Produits: {products_count:,}\n\n"
            
            # Date
            summary_text += f"Genere le:\n{datetime.now().strftime('%d/%m/%Y %H:%M')}"
            
        except:
            summary_text += "Données non disponibles"
        
        ax8.text(0.1, 0.5, summary_text, fontsize=11, 
                verticalalignment='center',
                bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.5))
        
        # Graphique 9: Comparaison prédictions vs historique
        try:
            ax9 = plt.subplot(3, 3, 9)
            
            # Données historiques (dernier mois)
            last_month = (datetime.now() - timedelta(days=30)).strftime("%Y-%m")
            pipeline_hist = [
                {"$match": {
                    "status": {"$regex": "livré", "$options": "i"},
                    "date": {"$gte": datetime.now() - timedelta(days=30)}
                }},
                {"$group": {
                    "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$date"}},
                    "daily_sales": {"$sum": "$total_amount"}
                }},
                {"$sort": {"_id": 1}}
            ]
            
            hist_data = list(db.orders.aggregate(pipeline_hist))
            
            if hist_data and 'predictions_data' in locals():
                # Moyenne historique
                hist_avg = np.mean([h['daily_sales'] for h in hist_data])
                
                # Moyenne prédictions
                pred_avg = np.mean(pred_sales) if 'pred_sales' in locals() and pred_sales else 0
                
                # Bar chart comparatif
                categories = ['Historique', 'Prédictions']
                values = [hist_avg, pred_avg]
                colors_comp = ['lightblue', 'lightgreen']
                
                bars = ax9.bar(categories, values, color=colors_comp, edgecolor='black')
                ax9.set_ylabel('Ventes moyennes (TND)')
                ax9.set_title('Comparaison Historique vs Prédictions')
                
                # Valeurs sur les barres
                for bar, value in zip(bars, values):
                    height = bar.get_height()
                    ax9.text(bar.get_x() + bar.get_width()/2., height,
                            f'{value:,.0f}', ha='center', va='bottom', fontweight='bold')
                
                # Différence en pourcentage
                if hist_avg > 0:
                    diff_pct = ((pred_avg - hist_avg) / hist_avg) * 100
                    trend_symbol = "↗" if diff_pct > 0 else "↘"
                    ax9.text(0.5, max(values)*1.1, 
                            f"{trend_symbol} {abs(diff_pct):.1f}%", 
                            ha='center', fontweight='bold', fontsize=12)
        except:
            # Alternative: carte simple
            ax9.text(0.5, 0.5, "Dashboard\nAnalytique\nBoutique", 
                    ha='center', va='center', fontsize=20, 
                    bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.7))
            ax9.axis('off')
        
        plt.tight_layout()
        
        # Sauvegarder
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f'dashboard_{timestamp}.png'
        plt.savefig(filename, dpi=150, bbox_inches='tight')
        print(f"Dashboard sauvegardé: {filename}")
        
        # Afficher
        plt.show()
        
    except Exception as e:
        print(f"Erreur lors de la création des graphiques: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*70)
    print("DASHBOARD TERMINÉ")
    print("="*70)

def quick_dashboard():
    """Dashboard rapide sans graphiques"""
    print("\n" + "="*70)
    print("DASHBOARD RAPIDE - BOUTIQUE")
    print("="*70)
    
    try:
        client = MongoClient('localhost', 27017)
        db = client['Boutique']
        
        print("Connecté à MongoDB")
        
        # Commandes
        total_orders = db.orders.count_documents({})
        delivered = db.orders.count_documents({"status": {"$regex": "livré", "$options": "i"}})
        
        print(f"\nCOMMANDES: {total_orders:,}")
        print(f"LIVREES: {delivered:,} ({delivered/total_orders*100:.1f}%)")
        
        # CA
        pipeline = [
            {"$match": {"status": {"$regex": "livré", "$options": "i"}}},
            {"$group": {
                "_id": None,
                "total": {"$sum": "$total_amount"},
                "avg": {"$avg": "$total_amount"}
            }}
        ]
        
        result = list(db.orders.aggregate(pipeline))
        if result:
            print(f"\nCHIFFRE D'AFFAIRES: {result[0]['total']:,.2f} TND")
            print(f"PANIER MOYEN: {result[0]['avg']:,.2f} TND")
        
        # Prédictions
        if "predictions" in db.list_collection_names():
            preds = list(db.predictions.find().sort("predicted_date", 1).limit(5))
            if preds:
                print(f"\nPROCHAINES PREDICTIONS:")
                for pred in preds:
                    date = pred.get('predicted_date', 'N/A')
                    amount = pred.get('predicted_sales', 0)
                    print(f"  {date}: {amount:,.2f} TND")
        
    except Exception as e:
        print(f"Erreur: {e}")

if __name__ == "__main__":
    print("TYPES DE DASHBOARD DISPONIBLES:")
    print("  1. Dashboard complet (avec graphiques)")
    print("  2. Dashboard rapide (sans graphiques)")
    
    choice = input("\nVotre choix (1-2, défaut: 1): ").strip()
    
    if choice == "2":
        quick_dashboard()
    else:
        create_dashboard()