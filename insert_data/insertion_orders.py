from pymongo import MongoClient
from datetime import datetime, timedelta
import random

client = MongoClient('mongodb://localhost:27017/')
db = client['Boutique']

# Clear existing data (optional)
db.orders.delete_many({})

print("Insertion des commandes...")

products_list = list(db.products.find())
customers_list = list(db.customers.find())

if not products_list or not customers_list:
    print("Veuillez d'abord insérer les produits et clients!")
    exit()

for i in range(1, 151):
    customer = random.choice(customers_list)
    
    num_items = random.randint(1, 4)
    selected_products = random.sample(products_list, min(num_items, len(products_list)))
    
    # CORRECT STRUCTURE: Array of objects
    items = []
    total_amount = 0
    
    for product in selected_products:
        quantity = random.randint(1, 3)
        subtotal = product["price"] * quantity
        
        # Create item as OBJECT
        item = {
            "product_id": product["product_id"],
            "name": product["name"],
            "price": product["price"],
            "quantity": quantity
        }
        items.append(item)
        total_amount += subtotal
    
    order_date = datetime.now() - timedelta(days=random.randint(0, 180))
    
    order = {
        "order_id": f"ORD-{i:04d}",
        "customer_id": customer["customer_id"],
        "date": order_date,
        "items": items,  # Array of OBJECTS
        "total_amount": total_amount,
        "payment_method": random.choice(["carte", "paypal", "livraison_contre_remboursement"]),
        "status": random.choice(["livré", "livré", "livré", "en cours", "en cours", "annulé"])
    }
    
    db.orders.insert_one(order)

print(f" {db.orders.count_documents({})} commandes insérées!")

# Statistiques
total_revenue = sum([order["total_amount"] for order in db.orders.find({"status": "livré"})])
print(f"Chiffre d'affaires (commandes livrées): {total_revenue:,.2f} TND")

status_counts = {}
for order in db.orders.find():
    status = order["status"]
    status_counts[status] = status_counts.get(status, 0) + 1

print(f"\nRépartition des commandes:")
for status, count in status_counts.items():
    print(f"  - {status}: {count}")

print("\n Toutes les données ont été insérées!")
print("Vérifiez dans MongoDB Compass")