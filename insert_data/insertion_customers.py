from pymongo import MongoClient
from datetime import datetime, timedelta
import random

# Connexion à MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['Boutique']

# Données de test
first_names = ["Ahmed", "Fatma", "Mohamed", "Sarra", "Ali", "Ines", "Youssef", "Leila", 
               "Omar", "Nour", "Karim", "Amira", "Mehdi", "Salma", "Rami"]
last_names = ["Ben Ali", "Trabelsi", "Jebali", "Gharbi", "Mansour", "Bouzid", 
              "Khelifi", "Hadj", "Nouira", "Farhat"]
cities = ["Tunis", "Sousse", "Sfax", "Nabeul", "Monastir", "Bizerte", "Kairouan"]
emails_domains = ["gmail.com", "yahoo.fr", "hotmail.com", "outlook.com"]

print("Insertion des clients...")

# Insérer 5 clients
customers_inserted = []
for i in range(1, 40):
    first_name = random.choice(first_names)
    last_name = random.choice(last_names)
    
    customer = {
        "customer_id": f"CUST-{i:03d}",
        "name": f"{first_name} {last_name}",
        "email": f"{first_name.lower()}.{last_name.lower().replace(' ', '')}@{random.choice(emails_domains)}",
        "phone": f"+216 {random.randint(20, 99)} {random.randint(100, 999)} {random.randint(100, 999)}",
        "join_date": datetime(2024, random.randint(1, 12), random.randint(1, 28)),
        "location": random.choice(cities)
    }
    
    result = db.customers.insert_one(customer)
    customers_inserted.append(customer["customer_id"])
    
print(f"{len(customers_inserted)} clients insérés avec succès!")
print(f"Base de données: {db.name}")
print(f"Collection: customers")