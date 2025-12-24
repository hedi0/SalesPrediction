from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['Boutique']

# Clear existing data (optional)
# db.products.delete_many({})

products_data = [
    {"product_id": "P-MT-001", "name": "Smartphone Tissot T900", "category": "téléphonie", 
     "description": "Smartphone local avec double SIM, 128GB", "price": 399, "stock": 25, "supplier": "MyTek"},
    {"product_id": "P-MT-002", "name": "TV Samsung 55\" 4K UHD", "category": "tv", 
     "description": "Télévision Samsung 55 pouces Smart TV", "price": 1599, "stock": 12, "supplier": "MyTek"},
    {"product_id": "P-MT-003", "name": "Laptop Lenovo IdeaPad 3", "category": "informatique", 
     "description": "Ordinateur portable 15.6\", 8GB RAM, 512GB SSD", "price": 1299, "stock": 18, "supplier": "MyTek"},
    {"product_id": "P-MT-004", "name": "Refrigerateur Samsung 350L", "category": "électroménager", 
     "description": "Réfrigérateur Samsung Inverter", "price": 1299, "stock": 8, "supplier": "MyTek"},
    {"product_id": "P-MT-005", "name": "Machine à laver LG 8kg", "category": "électroménager", 
     "description": "Machine à laver LG frontale 8kg", "price": 899, "stock": 15, "supplier": "MyTek"},
    {"product_id": "P-MT-006", "name": "Climatiseur Midea 12000 BTU", "category": "climatisation", 
     "description": "Climatiseur split Midea Inverter", "price": 1099, "stock": 10, "supplier": "MyTek"},
    {"product_id": "P-MT-007", "name": "Tablette Samsung Galaxy Tab A8", "category": "tablettes", 
     "description": "Tablette Samsung 10.5\", 64GB", "price": 599, "stock": 22, "supplier": "MyTek"},
    {"product_id": "P-MT-008", "name": "Smart TV LG 43\" 4K", "category": "tv", 
     "description": "Smart TV LG 43 pouces webOS", "price": 999, "stock": 14, "supplier": "MyTek"},
    {"product_id": "P-MT-009", "name": "Aspirateur Rowenta Silence Force", "category": "électroménager", 
     "description": "Aspirateur traîneau Rowenta 2000W", "price": 299, "stock": 30, "supplier": "MyTek"},
    {"product_id": "P-MT-010", "name": "PC Bureau Dell Vostro", "category": "informatique", 
     "description": "Tour PC Dell, i5, 16GB RAM, 1TB HDD", "price": 1899, "stock": 6, "supplier": "MyTek"},
    {"product_id": "P-MT-011", "name": "Imprimante HP LaserJet", "category": "informatique", 
     "description": "Imprimante laser HP multifonction", "price": 499, "stock": 20, "supplier": "MyTek"},
    {"product_id": "P-MT-012", "name": "Casque Gaming Razer", "category": "gaming", 
     "description": "Casque gaming avec micro et RGB", "price": 199, "stock": 35, "supplier": "MyTek"},
    {"product_id": "P-MT-013", "name": "Souris Logitech MX Master", "category": "informatique", 
     "description": "Souris ergonomique sans fil", "price": 129, "stock": 40, "supplier": "MyTek"},
    {"product_id": "P-MT-014", "name": "Clavier Mécanique Corsair", "category": "gaming", 
     "description": "Clavier mécanique RGB switches cherry", "price": 249, "stock": 25, "supplier": "MyTek"},
    {"product_id": "P-MT-015", "name": "Moniteur Dell 27\" 4K", "category": "informatique", 
     "description": "Écran 27 pouces 4K UHD IPS", "price": 899, "stock": 15, "supplier": "MyTek"}
]

print(" Insertion des produits...")

for product in products_data:
    db.products.insert_one(product)

print(f" {len(products_data)} produits insérés!")